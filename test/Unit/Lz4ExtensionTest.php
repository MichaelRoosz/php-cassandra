<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Compression\Lz4Compressor;
use Cassandra\Compression\Lz4Decompressor;
use Cassandra\Compression\Lz4Extension;

/**
 * Exercises the native-extension acceleration path and, crucially, its
 * cross-compatibility with the pure-PHP implementation in both directions — a
 * raw block produced by one must decode with the other. Skipped when the `lz4`
 * extension is not installed.
 */
class Lz4ExtensionTest extends AbstractUnitTestCase {
    protected function setUp(): void {
        if (!Lz4Extension::isAvailable()) {
            $this->markTestSkipped('The native lz4 extension is not available.');
        }
    }
    /**
     * @return array<string, array{0: string}>
     */
    public static function payloadProvider(): array {
        $cases = [
            'empty' => [''],
            'short' => ['hello lz4'],
            'exactly min-match' => ['abcd'],
            'text' => [str_repeat('The quick brown fox jumps over the lazy dog. ', 500)],
            'repetitive' => [str_repeat('0123456789abcdef', 4000)],
            'incompressible' => [random_bytes(9973)],
        ];

        // Sizes around the 64 KiB match window and the 128 KiB frame payload.
        foreach ([65535, 65536, 65537, 131071, 131072] as $size) {
            $cases["compressible {$size}"] = [substr(str_repeat('lz4-boundary;', (int) ceil($size / 13)), 0, $size)];
            $cases["incompressible {$size}"] = [random_bytes($size)];
        }

        return $cases;
    }

    /**
     * @dataProvider payloadProvider
     */
    public function testExtensionBlockDecodesWithPurePhp(string $input): void {
        $extBlock = (new Lz4Compressor(preferExtension: true))->compressBlock($input);

        $decompressor = new Lz4Decompressor(preferExtension: false);
        $decompressor->setInput($extBlock);

        $this->assertSame($input, $input === '' ? '' : $decompressor->decompressBlock());
    }

    /**
     * @dataProvider payloadProvider
     */
    public function testExtensionRoundTrip(string $input): void {
        $extBlock = (new Lz4Compressor(preferExtension: true))->compressBlock($input);

        $decompressor = new Lz4Decompressor(preferExtension: true);
        $decompressor->setInput($extBlock);

        $decoded = $input === '' ? '' : $decompressor->decompressBlock(strlen($input));

        $this->assertSame($input, $decoded);
    }

    /**
     * @dataProvider payloadProvider
     */
    public function testPurePhpBlockDecodesWithExtension(string $input): void {
        $pureBlock = (new Lz4Compressor(preferExtension: false))->compressBlock($input);

        $decompressor = new Lz4Decompressor(preferExtension: true);
        $decompressor->setInput($pureBlock);

        $decoded = $input === '' ? '' : $decompressor->decompressBlock(strlen($input));

        $this->assertSame($input, $decoded);
    }
}
