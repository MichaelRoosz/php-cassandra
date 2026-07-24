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
 * Thin adapter over the optional native LZ4 PHP extension (the `lz4` PECL
 * extension, e.g. kjdev/php-ext-lz4), used to accelerate raw LZ4 block
 * (de)compression when it is installed.
 *
 * The extension's `lz4_compress()` output is a 4-byte little-endian uncompressed
 * length followed by a raw LZ4 block; the CQL binary protocol uses just the raw
 * block. So compression strips that 4-byte prefix, and decompression prepends it
 * (the uncompressed length is always known from the frame header / body prefix).
 *
 * Availability is confirmed once, at first use, with a self-test that verifies
 * this exact framing so a future extension version with a different container
 * format can never silently corrupt data — it simply falls back to the pure-PHP
 * implementation.
 */
final class Lz4Extension {
    private static ?bool $available = null;

    /**
     * Compress a string into a raw LZ4 block, or null if the extension is
     * unavailable or failed.
     */
    public static function compressBlock(string $input): ?string {
        if (!self::isAvailable()) {
            return null;
        }

        $compressed = @lz4_compress($input, 0);

        return is_string($compressed) ? substr($compressed, 4) : null;
    }

    /**
     * Decompress a raw LZ4 block of known uncompressed length, or null if the
     * extension is unavailable or failed.
     */
    public static function decompressBlock(string $block, int $uncompressedLength): ?string {
        if (!self::isAvailable()) {
            return null;
        }

        $result = @lz4_uncompress(pack('V', $uncompressedLength) . $block);

        return is_string($result) ? $result : null;
    }

    public static function isAvailable(): bool {
        return self::$available ??= self::selfTest();
    }

    /**
     * Confirm the extension exists and produces the 4-byte-LE-prefixed container
     * this adapter assumes, so an incompatible build is rejected rather than
     * used.
     */
    private static function selfTest(): bool {
        if (!extension_loaded('lz4')) {
            return false;
        }

        $sample = 'php-cassandra-lz4-selftest-' . str_repeat('ab', 40);

        $compressed = @lz4_compress($sample, 0);
        if (!is_string($compressed) || strlen($compressed) < 4) {
            return false;
        }

        $rawBlock = substr($compressed, 4);
        $roundTrip = @lz4_uncompress(pack('V', strlen($sample)) . $rawBlock);

        return $roundTrip === $sample;
    }
}
