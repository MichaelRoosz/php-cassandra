<?php

/*
 * MIT License
 *
 * Copyright (c) 2026 Michael J. Roosz
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
*/

declare(strict_types=1);

namespace Cassandra\Compression;

/**
 * Pure-PHP LZ4 block compressor.
 *
 * Produces a raw LZ4 block (no frame header/footer) that is compatible with
 * {@see Lz4Decompressor::decompressBlock()} and with the block payload the
 * CQL binary protocol expects for LZ4 compression.
 *
 * The implementation uses a single fixed-size hash table and a greedy match
 * search (the classic "LZ4_compress_fast" strategy). It intentionally trades a
 * little compression ratio for speed, which matters because everything here
 * runs in plain PHP.
 */
final class Lz4Compressor {
    /**
     * LZ4 frame format constants used by {@see compress()}. The BD "block
     * maximum size" id 7 = 4 MiB; input is split into blocks no larger than this
     * so the produced frame is spec-conformant.
     */
    private const FRAME_BLOCK_MAX_SIZE = 4 * 1024 * 1024;
    private const FRAME_BLOCK_MAX_SIZE_ID = 7;
    private const FRAME_MAGIC = 0x184D2204;
    private const HASH_LOG = 16;

    /**
     * Multipliers for the two 16-bit halves of the 4-byte hash. Both are kept
     * below 2^15 so that (uint16 * multiplier) can never exceed a signed 32-bit
     * integer (0xFFFF * 0x7FFF < 2^31). This keeps the hash overflow-free — and
     * therefore identical — on both 32-bit and 64-bit PHP; the concrete values
     * only affect match-finding quality, not correctness (every candidate match
     * is byte-verified before it is emitted).
     */
    private const HASH_MULTIPLIER_HIGH = 0x5F65;
    private const HASH_MULTIPLIER_LOW = 0x7FE7;

    /**
     * Right-shift applied to the 31-bit mixed hash to bring its well-distributed
     * high bits down into the {@see HASH_LOG}-bit table index window.
     */
    private const HASH_SHIFT = 15;
    private const HASH_SIZE = 1 << self::HASH_LOG;

    private const LAST_LITERALS = 5;
    private const MATCH_LENGTH_BITS = 0x0F;

    /**
     * Maximum back-reference distance encodable in the 2-byte offset field.
     */
    private const MAX_DISTANCE = 0xFFFF;

    /**
     * "Match-finder limit": no match may start within this many bytes of the
     * end of the input; the tail is always emitted as literals. The LZ4 block
     * format mandates that the last match starts at least 12 bytes before the
     * end of the block (and the last 5 bytes are always literals), so this MUST
     * be 12 — emitting a match closer to the end produces a block that lenient
     * decoders accept but the reference decoder rejects.
     *
     * @see https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md ("Parsing restrictions")
     */
    private const MF_LIMIT = 12;

    private const MIN_MATCH = 4;

    private const RUN_MASK = 0x0F;

    private readonly bool $useExtension;

    /**
     * @param bool $preferExtension Use the native LZ4 PHP extension when it is
     *   available (much faster). Pass false to force the pure-PHP implementation,
     *   e.g. in tests that must exercise this code directly.
     */
    public function __construct(bool $preferExtension = true) {
        $this->useExtension = $preferExtension && Lz4Extension::isAvailable();
    }

    /**
     * Compress a string into a complete, standards-compliant LZ4 frame (spec
     * v1.6.x, magic 0x184D2204) that any conforming LZ4 tool can decode — the
     * counterpart to {@see Lz4Decompressor::decompress()}.
     *
     * The frame uses block-independent 4 MiB blocks with no block or content
     * checksums. Each block is produced by {@see compressBlock()} (so it also
     * benefits from the native extension when available); a block that would not
     * shrink is stored uncompressed.
     */
    public function compress(string $input): string {
        // Frame descriptor: FLG = version 01 + block independence (no checksums,
        // no content size); BD = 4 MiB block-max-size id.
        $descriptor = chr(0x60) . chr(self::FRAME_BLOCK_MAX_SIZE_ID << 4);

        // Header checksum = (XXH32(descriptor, seed 0) >> 8) & 0xFF = byte 2.
        $headerChecksum = ord(hash('xxh32', $descriptor, true, ['seed' => 0])[2]);

        $frame = pack('V', self::FRAME_MAGIC) . $descriptor . chr($headerChecksum);

        $length = strlen($input);
        $offset = 0;
        while ($offset < $length) {
            $chunk = substr($input, $offset, self::FRAME_BLOCK_MAX_SIZE);
            $offset += strlen($chunk);

            $block = $this->compressBlock($chunk);

            if (strlen($block) >= strlen($chunk)) {
                // Incompressible: store the chunk uncompressed, flagged by the
                // high bit of the 4-byte block size. Set that bit on the packed
                // size's most-significant (little-endian, 4th) byte rather than
                // OR-ing 0x80000000, which overflows a signed int on 32-bit PHP.
                $blockSize = pack('V', strlen($chunk));
                $blockSize[3] = chr((ord($blockSize[3]) | 0x80) & 0xFF);
                $frame .= $blockSize . $chunk;
            } else {
                $frame .= pack('V', strlen($block)) . $block;
            }
        }

        // EndMark.
        return $frame . pack('V', 0);
    }

    /**
     * Compress a string into a single raw LZ4 block.
     */
    public function compressBlock(string $input): string {
        if ($this->useExtension) {
            $compressed = Lz4Extension::compressBlock($input);
            if ($compressed !== null) {
                return $compressed;
            }
        }

        $inputLength = strlen($input);

        // Too small to hold even one match plus the mandatory trailing
        // literals: emit the whole input as a single literal run.
        if ($inputLength < self::MF_LIMIT + 1) {
            return $this->encodeLastLiterals($input, 0, $inputLength);
        }

        $matchLimit = $inputLength - self::LAST_LITERALS;
        $mfLimit = $inputLength - self::MF_LIMIT;

        /** @var array<int, int> $hashTable */
        $hashTable = [];

        $output = '';
        $anchor = 0;
        $pos = 0;

        // Rolling 4-byte lookahead window, held as two 16-bit halves:
        // $windowLow = bytes [pos, pos+1], $windowHigh = bytes [pos+2, pos+3].
        // Advancing one byte shifts a single new byte into the window, which is
        // far cheaper than re-reading four bytes with unpack() at every position
        // — the dominant cost when scanning incompressible (or barely
        // compressible) data. The window is only reloaded from scratch after a
        // match skips $pos forward. Keeping the halves separate also preserves
        // 32-bit safety: every value stays within 16 bits (see HASH_MULTIPLIER_*).
        $windowLow = ord($input[0]) | (ord($input[1]) << 8);
        $windowHigh = ord($input[2]) | (ord($input[3]) << 8);

        while ($pos < $mfLimit) {
            $hash = ((($windowLow * self::HASH_MULTIPLIER_LOW) ^ ($windowHigh * self::HASH_MULTIPLIER_HIGH)) >> self::HASH_SHIFT)
                & (self::HASH_SIZE - 1);

            $ref = $hashTable[$hash] ?? -1;
            $hashTable[$hash] = $pos;

            if (
                $ref < 0
                || ($pos - $ref) > self::MAX_DISTANCE
                || substr($input, $ref, self::MIN_MATCH) !== substr($input, $pos, self::MIN_MATCH)
            ) {
                // Advance one byte: drop the lowest byte and shift in the byte
                // that now enters the top of the window (input[$pos + 3] after
                // the increment). $windowLow must be updated before $windowHigh,
                // as it consumes the old low byte of $windowHigh.
                $pos++;
                $windowLow = ($windowLow >> 8) | (($windowHigh & 0xFF) << 8);
                $windowHigh = ($windowHigh >> 8) | (ord($input[$pos + 3]) << 8);

                continue;
            }

            // Extend the match forward as far as the input allows.
            $matchLength = self::MIN_MATCH;
            while ($pos + $matchLength < $matchLimit && $input[$ref + $matchLength] === $input[$pos + $matchLength]) {
                $matchLength++;
            }

            $output .= $this->encodeSequence($input, $anchor, $pos, $pos - $ref, $matchLength);

            $pos += $matchLength;
            $anchor = $pos;

            // The match skipped $pos forward, so the rolling window is stale;
            // reload all four bytes at the new position in one unpack().
            if ($pos < $mfLimit) {
                /** @var false|array<int> $halves */
                $halves = unpack('v2', $input, $pos);
                if ($halves === false) {
                    break;
                }
                $windowLow = $halves[1];
                $windowHigh = $halves[2];
            }
        }

        $output .= $this->encodeLastLiterals($input, $anchor, $inputLength);

        return $output;
    }

    /**
     * Encode the trailing literal run that terminates every LZ4 block.
     */
    private function encodeLastLiterals(string $input, int $anchor, int $end): string {
        $literalLength = $end - $anchor;

        if ($literalLength >= self::RUN_MASK) {
            $token = self::RUN_MASK << 4;
            $output = chr($token) . $this->encodeLength($literalLength - self::RUN_MASK);
        } else {
            $output = chr(($literalLength << 4) & 0xFF);
        }

        return $output . substr($input, $anchor, $literalLength);
    }

    /**
     * Encode an extended length value (used for both literal and match
     * lengths) as a sequence of 0xFF bytes followed by the remainder.
     */
    private function encodeLength(int $length): string {
        $bytes = '';
        while ($length >= 255) {
            $bytes .= "\xFF";
            $length -= 255;
        }

        return $bytes . chr($length & 0xFF);
    }

    /**
     * Encode one (literals + match) sequence.
     */
    private function encodeSequence(string $input, int $anchor, int $matchPos, int $offset, int $matchLength): string {
        $literalLength = $matchPos - $anchor;
        $encodedMatchLength = $matchLength - self::MIN_MATCH;

        $literalToken = $literalLength >= self::RUN_MASK ? self::RUN_MASK : $literalLength;
        $matchToken = $encodedMatchLength >= self::MATCH_LENGTH_BITS ? self::MATCH_LENGTH_BITS : $encodedMatchLength;

        $output = chr((($literalToken << 4) | $matchToken) & 0xFF);

        if ($literalLength >= self::RUN_MASK) {
            $output .= $this->encodeLength($literalLength - self::RUN_MASK);
        }

        $output .= substr($input, $anchor, $literalLength);

        // 2-byte little-endian back-reference offset.
        $output .= chr($offset & 0xFF) . chr(($offset >> 8) & 0xFF);

        if ($encodedMatchLength >= self::MATCH_LENGTH_BITS) {
            $output .= $this->encodeLength($encodedMatchLength - self::MATCH_LENGTH_BITS);
        }

        return $output;
    }
}
