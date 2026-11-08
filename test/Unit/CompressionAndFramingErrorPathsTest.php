<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Compression\Lz4Compressor;
use Cassandra\Compression\Lz4Decompressor;
use Cassandra\Compression\Lz4Extension;
use Cassandra\Connection\FrameCodec;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\Exception\CassandraException;
use Cassandra\Exception\CompressionException;
use Cassandra\Exception\NodeException;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Query;
use Cassandra\Test\Support\Faults;
use Closure;
use PHPUnit\Framework\Attributes\DataProvider;
use ReflectionProperty;

/**
 * LZ4 frames and blocks, and protocol v5 segments, broken one field at a time.
 */
final class CompressionAndFramingErrorPathsTest extends AbstractUnitTestCase {
    private const FRAME_MAGIC = "\x04\x22\x4D\x18";
    private const LEGACY_MAGIC = "\x02\x21\x4C\x18";
    private const SKIPPABLE_MAGIC = "\x50\x2A\x4D\x18";

    protected function setUp(): void {
        parent::setUp();

        if (PHP_INT_SIZE < 8) {
            $this->markTestSkipped('needs 64-bit integers');
        }
    }

    protected function tearDown(): void {
        Faults::reset();
        (new ReflectionProperty(Lz4Extension::class, 'available'))->setValue(null, null);
        parent::tearDown();
    }

    /**
     * @return array<string, array{string}>
     */
    public static function brokenFrames(): array {
        if (PHP_INT_SIZE < 8) {
            return ['needs 64-bit integers' => ['']];
        }

        $good = self::frame('hello world', true, true, true);
        $headerEnd = 4 + 2 + 8;

        return [
            'magic cut short' => ["\x04\x22"],
            'unknown magic' => ["\x00\x00\x00\x00"],
            'skippable frame without its size' => [self::SKIPPABLE_MAGIC . "\x01"],
            'descriptor cut short' => [self::FRAME_MAGIC . "\x40"],
            'unsupported version' => [self::FRAME_MAGIC . "\x80\x40\x00"],
            'content size cut short' => [self::FRAME_MAGIC . "\x48\x40\x01\x02"],
            'header checksum missing' => [substr($good, 0, $headerEnd)],
            'header checksum wrong' => [substr_replace($good, chr(~ord($good[$headerEnd]) & 0xFF), $headerEnd, 1)],
            'block size missing' => [substr($good, 0, $headerEnd + 1)],
            'block longer than the input' => [substr($good, 0, $headerEnd + 1 + 4 + 5)],
            'block checksum missing' => [substr($good, 0, $headerEnd + 1 + 4 + 11)],
            'block checksum wrong' => [substr_replace($good, "\0\0\0\0", $headerEnd + 1 + 4 + 11, 4)],
            'content checksum missing' => [substr($good, 0, strlen($good) - 4)],
            'content checksum wrong' => [substr($good, 0, -4) . "\0\0\0\0"],
            'legacy block cut short' => [self::LEGACY_MAGIC . pack('V', 50) . 'xy'],
        ];
    }

    /**
     * A frame with one stored block, and optionally block and content checksums.
     */
    public static function frame(string $content, bool $blockChecksum = false, bool $contentChecksum = false, bool $contentSize = false): string {
        $flg = 0x40 | 0x20 | ($blockChecksum ? 0x10 : 0) | ($contentSize ? 0x08 : 0) | ($contentChecksum ? 0x04 : 0);
        $descriptor = chr($flg & 0xFF) . chr(0x40) . ($contentSize ? pack('VV', strlen($content), 0) : '');
        $frame = self::FRAME_MAGIC . $descriptor . chr((self::xxh32($descriptor) >> 8) & 0xFF);
        $frame .= pack('V', strlen($content) | 0x80000000) . $content;
        if ($blockChecksum) {
            $frame .= pack('V', self::xxh32($content));
        }
        $frame .= pack('V', 0);
        if ($contentChecksum) {
            $frame .= pack('V', self::xxh32($content));
        }

        return $frame;
    }

    #[DataProvider('brokenFrames')]
    public function testABrokenFrameIsRefused(string $frame): void {
        $this->expectException(CompressionException::class);
        (new Lz4Decompressor($frame, preferExtension: false))->decompress();
    }

    #[DataProvider('unpackFailures')]
    public function testAFailedUnpackIsReported(string $format, string $frame): void {
        // Each unpack of that format fails in turn, so every step is reached once.
        for ($skip = 0; $skip < 6; $skip++) {
            Faults::reset();
            $seen = 0;
            Faults::injectWhere('unpack', static function (mixed $f) use ($format, &$seen, $skip): bool {
                return is_string($f) && str_starts_with($f, $format) && $seen++ === $skip;
            }, false);

            try {
                (new Lz4Decompressor($frame, preferExtension: false))->decompress();
            } catch (CompressionException) {
            }
        }
        $this->addToAssertionCount(1);
    }

    public function testASegmentHeaderThatCannotBeUnpacked(): void {
        $header = substr(pack('V', 9 | (1 << 17)), 0, 3);
        $plain = $header . substr(pack('V', self::crc24($header)), 0, 3);
        $header = substr(pack('VV', 9 | (9 << 17), 1 << 2), 0, 5);
        $compressed = $header . substr(pack('V', self::crc24($header)), 0, 3);

        foreach (['decodeUncompressedFrameHeader' => [$plain, ''], 'decodeCompressedFrameHeader' => [$compressed, 'lz4']] as $decoder => [$bytes, $compression]) {
            Faults::injectWhere('unpack', static fn (): bool => in_array($decoder, array_column(debug_backtrace(), 'function'), true), false);
            $this->assertRaises(NodeException::class, static fn () => (new FrameCodec(new BytesNode($bytes), $compression))->readAvailableDataFromSource(9, 9, null));
            Faults::reset();
        }
    }

    public function testBrokenV5Segments(): void {
        $codec = static fn (string $bytes, string $compression = ''): FrameCodec => new FrameCodec(new BytesNode($bytes), $compression);
        $segment = static function (string $payload, bool $breakHeader = false, bool $breakPayload = false): string {
            $header = substr(pack('V', strlen($payload) | (1 << 17)), 0, 3);
            $crc24 = self::crc24($header) ^ ($breakHeader ? 1 : 0);
            $crc32 = crc32("\xFA\x2D\x55\xCA" . $payload) ^ ($breakPayload ? 1 : 0);

            return $header . substr(pack('V', $crc24), 0, 3) . $payload . strrev(pack('N', $crc32));
        };
        $read = static fn (FrameCodec $c): string => $c->readAvailableDataFromSource(9, 9, null);

        $this->assertRaises(NodeException::class, static fn () => new FrameCodec(new BytesNode(''), 'snappy'));
        $this->assertRaises(NodeException::class, fn () => $read($codec($segment('abcdefghi', breakHeader: true))));
        $this->assertRaises(NodeException::class, fn () => $read($codec($segment('abcdefghi', breakPayload: true))));

        // A compressed segment whose declared length does not match what it decompresses to.
        $block = (new Lz4Compressor(preferExtension: false))->compressBlock(str_repeat('a', 40));
        $header = substr(pack('P', strlen($block) | (20 << 17) | (1 << 34)), 0, 5);
        $compressed = $header . substr(pack('V', self::crc24($header)), 0, 3) . $block . strrev(pack('N', crc32("\xFA\x2D\x55\xCA" . $block)));
        $this->assertRaises(CassandraException::class, fn () => $read($codec($compressed, 'lz4')));

        foreach (['V', 'N'] as $format) {
            for ($skip = 0; $skip < 3; $skip++) {
                Faults::reset();
                $seen = 0;
                Faults::injectWhere('unpack', static fn (mixed $f): bool => is_string($f) && str_starts_with($f, $format) && $seen++ === $skip, false);

                try {
                    $read($codec($segment('abcdefghi')));
                    $read($codec($compressed, 'lz4'));
                } catch (CassandraException) {
                }
            }
        }

        Faults::reset();
        $writer = new FrameCodec(new BytesNode(''), 'lz4');
        $writer->write('raw');
        $request = new Query('SELECT 1');
        $request->setVersion(ProtocolVersion::V5);
        $request->setStream(1);
        Faults::injectWhere('unpack', static fn (mixed $f): bool => $f === 'N' && in_array('writeFrame', array_column(debug_backtrace(), 'function'), true), false);
        $this->assertRaises(NodeException::class, static fn () => $writer->writeRequest($request));
    }

    public function testRawBlocksThatEndMidSequence(): void {
        // One literal, then an offset that is not there.
        $this->assertRaises(CompressionException::class, static fn () => (new Lz4Decompressor("\x10a\x01", preferExtension: false))->decompressBlock(5));
        // A match length extension that is not there.
        $this->assertRaises(CompressionException::class, static fn () => (new Lz4Decompressor("\x1Fa\x01\x00", preferExtension: false))->decompressBlock(30));
    }

    public function testTheExtensionAdapterWhenTheExtensionIsMissingOrBroken(): void {
        $available = new ReflectionProperty(Lz4Extension::class, 'available');

        $available->setValue(null, false);
        $this->assertNull(Lz4Extension::compressBlock('abc'));
        $this->assertNull(Lz4Extension::decompressBlock('abc', 3));

        if (extension_loaded('lz4')) {
            $available->setValue(null, null);
            Faults::inject('lz4_compress', false);
            $this->assertFalse(Lz4Extension::isAvailable());
        }

        Faults::injectWhere('unpack', static fn (mixed $f): bool => $f === 'V2', false);
        $this->assertNotSame('', (new Lz4Compressor(preferExtension: false))->compressBlock(str_repeat('abcdefgh', 64)));
    }

    public function testV5SegmentsArrivingInPieces(): void {
        $quiet = static function (string $bytes): BytesNode {
            $node = new BytesNode($bytes);
            $node->quietWhenEmpty = true;

            return $node;
        };
        $read = static fn (FrameCodec $c): string => $c->readAvailableDataFromSource(9, 9, null);

        foreach (['', 'lz4'] as $compression) {
            $node = $quiet('');
            $codec = new FrameCodec($node, $compression);
            $this->assertSame('', $read($codec));
            $this->assertSame(0, $codec->getReceivedByteCount());
            $codec->close();
            $this->assertTrue($node->closed);
        }

        // A header whose payload has not arrived yet.
        $header = substr(pack('V', 9 | (1 << 17)), 0, 3);
        $this->assertSame('', $read(new FrameCodec($quiet($header . substr(pack('V', self::crc24($header)), 0, 3)), '')));

        // A compressed segment that decompresses to less than its header declares.
        $block = (new Lz4Compressor(preferExtension: false))->compressBlock(str_repeat('a', 20));
        $header = substr(pack('VV', strlen($block) | (40 << 17), 1 << 2), 0, 5);
        $compressed = $header . substr(pack('V', self::crc24($header)), 0, 3) . $block . strrev(pack('N', crc32("\xFA\x2D\x55\xCA" . $block)));
        $this->assertRaises(NodeException::class, fn () => $read(new FrameCodec(new BytesNode($compressed), 'lz4')));
    }

    public function testValidFramesDecodeWithEveryOptionalField(): void {
        $this->assertSame('hello world', (new Lz4Decompressor(self::frame('hello world', true, true, true), preferExtension: false))->decompress());
        $skippable = self::SKIPPABLE_MAGIC . pack('V', 3) . 'abc';
        $this->assertSame('hi', (new Lz4Decompressor($skippable . self::frame('hi'), preferExtension: false))->decompress());
    }

    /**
     * @return array<string, array{string, string}>
     */
    public static function unpackFailures(): array {
        if (PHP_INT_SIZE < 8) {
            return ['needs 64-bit integers' => ['', '']];
        }

        return [
            'magic' => ['V', self::frame('x')],
            'skippable size' => ['V', self::SKIPPABLE_MAGIC . pack('V', 1) . 'a'],
            'legacy block size' => ['V', self::LEGACY_MAGIC . pack('V', 1) . 'a'],
            'block size' => ['V', self::frame('x')],
            'block checksum' => ['V', self::frame('x', blockChecksum: true)],
            'content checksum' => ['V', self::frame('x', contentChecksum: true)],
            'checksum computation' => ['N', self::frame('x', blockChecksum: true)],
            'header checksum' => ['C', self::frame('x')],
        ];
    }

    /**
     * @param class-string<CassandraException> $expected
     * @param Closure(): mixed $call
     */
    private function assertRaises(string $expected, Closure $call): void {
        try {
            $call();
            $this->fail("expected $expected");
        } catch (CassandraException $e) {
            $this->assertInstanceOf($expected, $e, $e->getMessage());
        }
    }

    private static function crc24(string $data): int {
        $crc = 0x875060;
        for ($i = 0; $i < strlen($data); $i++) {
            $crc ^= ord($data[$i]) << 16;
            for ($bit = 0; $bit < 8; $bit++) {
                $crc <<= 1;
                if ($crc & 0x1000000) {
                    $crc ^= 0x1974F0B;
                }
            }
        }

        return $crc & 0xFFFFFF;
    }

    private static function xxh32(string $data): int {
        return (int) hexdec(hash('xxh32', $data));
    }
}

/**
 * Serves a fixed byte string, then reports a read timeout.
 */
final class BytesNode extends \Cassandra\Connection\NodeImplementation {
    public bool $closed = false;

    /**
     * Whether running out of bytes reads as nothing available rather than a read timeout.
     */
    public bool $quietWhenEmpty = false;

    public string $written = '';

    public function __construct(private string $bytes) {
    }

    public function close(): void {
        $this->closed = true;
    }

    public function getConfig(): \Cassandra\Connection\NodeConfig {
        return new StreamNodeConfig();
    }

    public function readAvailableDataFromSource(int $expectedLength, int $upperBoundaryLength, ?float $readDeadline): string {
        if ($this->bytes === '' && $this->quietWhenEmpty) {
            return '';
        }
        if ($this->bytes === '') {
            throw new \Cassandra\Exception\StreamException('no more bytes', \Cassandra\Exception\ExceptionCode::STREAM_TIMEOUT_DURING_READ->value);
        }
        $chunk = substr($this->bytes, 0, $upperBoundaryLength);
        $this->bytes = substr($this->bytes, strlen($chunk));

        return $chunk;
    }

    public function write(string $data): void {
        $this->written .= $data;
    }

    public function writeRequest(\Cassandra\Request\Request $request): void {
        $this->written .= (string) $request;
    }
}
