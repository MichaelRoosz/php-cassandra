<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Compression\Lz4Compressor;
use Cassandra\Compression\Lz4Decompressor;

class Lz4CompressorTest extends AbstractUnitTestCase {
    /**
     * @return array<string, array{0: string}>
     */
    public static function parsingRestrictionsProvider(): array {
        // A 4-byte sequence ("MNOP") that recurs 10 bytes before the end. With the
        // buggy MF_LIMIT of 9 the compressor emitted a match starting there (only
        // 10 bytes before the end), which is invalid LZ4.
        $endMatchTrigger = 'MNOP' . str_repeat('.', 30) . 'MNOP......';

        $cases = [
            'end-match trigger' => [$endMatchTrigger],
            'repetitive' => [str_repeat('abcd', 4000)],
            'text with repeats' => [str_repeat('The quick brown fox. ', 500)],
            'random 9973' => [random_bytes(9973)],
            'random 131073' => [random_bytes(131073)],
        ];

        // Sequences whose final bytes repeat an earlier run, at every tail offset
        // that could tempt a too-late match.
        for ($tail = 6; $tail <= 16; $tail++) {
            $data = 'WXYZ' . str_repeat('-', 40) . 'WXYZ' . str_repeat('=', $tail - 4);
            $cases["tail repeat -{$tail}"] = [$data];
        }

        return $cases;
    }
    /**
     * @return array<string, array{0: string}>
     *
     * @throws \Random\RandomException
     */
    public static function frameRoundTripProvider(): array {
        return [
            'empty' => [''],
            'short' => ['hello lz4 frame'],
            'compressible' => [str_repeat('The quick brown fox. ', 5000)],
            'incompressible' => [random_bytes(70000)],
            // Larger than the 4 MiB frame block max: exercises multiple blocks.
            'multi-block compressible' => [str_repeat('0123456789abcdef', 300000)],
            // Multiple blocks that are each stored uncompressed.
            'multi-block incompressible' => [random_bytes(5 * 1024 * 1024)],
        ];
    }

    /**
     * @return array<string, array{0: string}>
     */
    public static function roundTripProvider(): array {
        return [
            'empty' => [''],
            'single byte' => ['a'],
            'below min match' => ['abc'],
            'exactly min match' => ['abcd'],
            'short string' => ['hello world'],
            'cql insert' => ['INSERT INTO kv (id, v) VALUES (?, ?)'],
            'highly repetitive' => [str_repeat('a', 5000)],
            'repeated token' => [str_repeat('abcd', 4000)],
            'repeated sentence' => [str_repeat('The quick brown fox jumps over the lazy dog. ', 500)],
            'repeated block' => [str_repeat('0123456789abcdef', 8192)],
        ];
    }

    public function testCompressBlockOfEmptyStringIsDecodableAndEmpty(): void {
        $compressor = new Lz4Compressor(preferExtension: false);
        $decompressor = new Lz4Decompressor();

        $compressed = $compressor->compressBlock('');
        $decompressor->setInput($compressed);

        $this->assertSame('', $decompressor->decompressBlock());
    }

    /**
     * Regression guard for the LZ4 block "parsing restrictions": the last match
     * must start at least 12 bytes before the end of the block, and the last 5
     * bytes must be literals. Violating them produces a block that our lenient
     * decompressor accepts but the reference decoder rejects, so this is checked
     * structurally rather than by round-tripping.
     *
     * @dataProvider parsingRestrictionsProvider
     */
    public function testCompressedBlockObeysLz4ParsingRestrictions(string $input): void {
        $compressor = new Lz4Compressor(preferExtension: false);
        $block = $compressor->compressBlock($input);

        [$outputLength, $lastMatchStart, $trailingLiterals] = $this->parseBlock($block);

        $this->assertSame(strlen($input), $outputLength, 'block does not reconstruct to the original length');

        if ($lastMatchStart !== null) {
            $this->assertLessThanOrEqual(
                $outputLength - 12,
                $lastMatchStart,
                'last match starts within 12 bytes of the end of the block'
            );
            $this->assertGreaterThanOrEqual(
                5,
                $trailingLiterals,
                'the last 5 bytes of the block must be literals'
            );
        }
    }

    public function testCompressibleDataActuallyShrinks(): void {
        $compressor = new Lz4Compressor(preferExtension: false);

        $input = str_repeat('The quick brown fox jumps over the lazy dog. ', 1000);
        $compressed = $compressor->compressBlock($input);

        $this->assertLessThan(strlen($input), strlen($compressed));
    }

    /**
     * @dataProvider roundTripProvider
     */
    public function testRoundTrip(string $input): void {
        $compressor = new Lz4Compressor(preferExtension: false);
        $decompressor = new Lz4Decompressor();

        $compressed = $compressor->compressBlock($input);

        $decompressor->setInput($compressed);
        $decompressed = $decompressor->decompressBlock();

        $this->assertSame($input, $decompressed);
    }

    /**
     * compress() must produce a standard LZ4 frame that our own frame decoder
     * reads back byte-for-byte, including the multi-block case (input larger than
     * the 4 MiB frame block max) and stored-uncompressed incompressible blocks.
     *
     * @dataProvider frameRoundTripProvider
     */
    public function testFrameRoundTrip(string $input): void {
        $compressor = new Lz4Compressor(preferExtension: false);

        $frame = $compressor->compress($input);
        $decompressed = (new Lz4Decompressor($frame))->decompress();

        $this->assertSame($input, $decompressed);
    }

    public function testRoundTripOfBinaryDataLargerThanOneFrame(): void {
        $compressor = new Lz4Compressor(preferExtension: false);
        $decompressor = new Lz4Decompressor();

        // Mix of compressible and incompressible data, larger than the
        // 128 KiB frame payload limit, to exercise long matches and offsets.
        $input = '';
        for ($i = 0; $i < 4000; $i++) {
            $input .= pack('N', $i) . 'row-' . $i . '-payload;';
        }

        $compressed = $compressor->compressBlock($input);
        $decompressor->setInput($compressed);

        $this->assertSame($input, $decompressor->decompressBlock());
    }

    public function testRoundTripOfRandomIncompressibleData(): void {
        $compressor = new Lz4Compressor(preferExtension: false);
        $decompressor = new Lz4Decompressor();

        $input = random_bytes(9973);

        $compressed = $compressor->compressBlock($input);
        $decompressor->setInput($compressed);

        $this->assertSame($input, $decompressor->decompressBlock());
    }

    /**
     * Walk a raw LZ4 block and return [outputLength, lastMatchStartOffset,
     * trailingLiteralCount]. lastMatchStartOffset is the output position where
     * the final match begins, or null if the block contains only literals.
     *
     * @return array{0: int, 1: ?int, 2: int}
     */
    private function parseBlock(string $block): array {
        $length = strlen($block);
        $offset = 0;
        $outputLength = 0;
        $lastMatchStart = null;
        $trailingLiterals = 0;

        while ($offset < $length) {
            $token = ord($block[$offset++]);

            $literals = $token >> 4;
            if ($literals === 0xF) {
                do {
                    $summand = ord($block[$offset++]);
                    $literals += $summand;
                } while ($summand === 0xFF);
            }

            $offset += $literals;
            $outputLength += $literals;
            $trailingLiterals = $literals;

            if ($offset >= $length) {
                break;
            }

            // Match: 2-byte offset then optional extended length.
            $offset += 2;

            $matchLength = $token & 0xF;
            if ($matchLength === 0xF) {
                do {
                    $summand = ord($block[$offset++]);
                    $matchLength += $summand;
                } while ($summand === 0xFF);
            }
            $matchLength += 4;

            $lastMatchStart = $outputLength;
            $outputLength += $matchLength;
            $trailingLiterals = 0;
        }

        return [$outputLength, $lastMatchStart, $trailingLiterals];
    }
}
