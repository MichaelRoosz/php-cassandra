<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Compression\Lz4Compressor;
use Cassandra\Compression\Lz4Decompressor;
use Cassandra\Exception\CompressionException;
use Cassandra\Exception\ExceptionCode;
use ReflectionMethod;

/**
 * A length read out of an LZ4 frame has to be refused when it names more bytes
 * than are left — a negative one included.
 *
 * The lengths in this format are unsigned 32-bit fields, and unpack('V') hands
 * anything from 0x80000000 up back as a negative int on 32-bit PHP. The
 * skippable-frame size was tested against an upper bound alone, which a negative
 * number passes: the frame was accepted and the read offset moved *backwards*,
 * so decompress() went round again over input it had already consumed — a
 * crafted frame landing on a skippable magic each time round never terminated.
 * The legacy block size had the milder form of the same fault, being read as a
 * block of no bytes and the malformed frame accepted as an empty one.
 *
 * The frames below declare 0xFFFFFFFF, which is that negative value on 32-bit
 * and an impossibly large one on 64-bit; either way it names more than the input
 * holds and either way it has to be refused, which is what makes these tests say
 * the same thing on both architectures.
 */
final class Lz4FrameLengthBoundTest extends AbstractUnitTestCase {
    /** The magic number of the pre-1.4 "legacy" frame format. */
    private const MAGIC_LEGACY = 0x184C2102;

    /** The lowest of the skippable-frame magic numbers. */
    private const MAGIC_SKIPPABLE = 0x184D2A50;

    /**
     * The invariant behind both of the above, stated where the architecture
     * cannot hide it: on 64-bit PHP no unpack('V') ever produces a negative
     * length, so only this reaches the branch that the two frame tests exercise
     * on a 32-bit build.
     */
    public function testANegativeLengthCountsAsTruncated(): void {
        $decompressor = new Lz4Decompressor(str_repeat("\x00", 64), 0, 64, false);

        $isTruncatedLength = new ReflectionMethod(Lz4Decompressor::class, 'isTruncatedLength');

        $this->assertTrue($isTruncatedLength->invoke($decompressor, -1), 'a negative length names no bytes that exist');
        $this->assertTrue($isTruncatedLength->invoke($decompressor, PHP_INT_MIN));
        $this->assertFalse($isTruncatedLength->invoke($decompressor, 0));
        $this->assertFalse($isTruncatedLength->invoke($decompressor, 64));
        $this->assertTrue($isTruncatedLength->invoke($decompressor, 65), 'one byte past the end is past the end');
    }

    public function testLegacyFrameWithAnOversizedBlockSizeIsRefused(): void {
        $frame = pack('V', self::MAGIC_LEGACY) . "\xFF\xFF\xFF\xFF" . 'payload';

        $decompressor = new Lz4Decompressor($frame, 0, strlen($frame), false);

        $this->expectException(CompressionException::class);
        $this->expectExceptionCode(ExceptionCode::COMPRESSION_INPUT_OVERFLOW->value);

        $decompressor->decompress(false);
    }

    /**
     * The added bound must not cost a frame that is simply skippable: one whose
     * size really does fit is stepped over and the frame after it decoded.
     */
    public function testSkippableFrameOfAValidSizeIsStillSkipped(): void {
        $skipped = 'metadata that is not lz4 at all';

        $frame = pack('V', self::MAGIC_SKIPPABLE) . pack('V', strlen($skipped)) . $skipped;

        $decompressor = new Lz4Decompressor($frame, 0, strlen($frame), false);

        $this->assertSame('', $decompressor->decompress(false), 'a skippable frame contributes no output');

        // And again with a real frame behind it, so the offset it left behind is
        // shown to be the right one rather than merely not raising.
        $payload = str_repeat('php-cassandra ', 64);
        $withContent = $frame . (new Lz4Compressor())->compress($payload);

        $decompressor = new Lz4Decompressor($withContent, 0, strlen($withContent), false);

        $this->assertSame($payload, $decompressor->decompress(true));
    }

    public function testSkippableFrameWithAnOversizedSizeIsRefused(): void {
        $frame = pack('V', self::MAGIC_SKIPPABLE) . "\xFF\xFF\xFF\xFF" . 'payload';

        $decompressor = new Lz4Decompressor($frame, 0, strlen($frame), false);

        $this->expectException(CompressionException::class);
        $this->expectExceptionCode(ExceptionCode::COMPRESSION_INPUT_OVERFLOW->value);

        $decompressor->decompress(false);
    }
}
