<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Exception\CompressionException as CompressionException;
use Cassandra\Compression\Lz4Compressor;
use Cassandra\Compression\Lz4Decompressor;
use Cassandra\Exception\ExceptionCode;

class Lz4DecompressorTest extends AbstractUnitTestCase {
    private const MAGIC_LEGACY = 0x184C2102;
    private const MAGIC_V1 = 0x184D2204;

    /**
     * @return array<string, array{int<0, 255>, int<0, 255>}>
     */
    public static function invalidFrameDescriptorProvider(): array {
        return [
            'reserved FLG bit' => [0x62, 0x40],
            'reserved BD bit' => [0x60, 0x41],
            'unsupported block maximum id' => [0x60, 0x00],
        ];
    }

    public function testDecompressBlockRejectsLiteralsBeyondExpectedLength(): void {
        $dec = new Lz4Decompressor("\x20ab", preferExtension: false);

        try {
            $dec->decompressBlock(1);
            $this->fail('expected literals beyond the declared output length to be refused');
        } catch (CompressionException $e) {
            $this->assertSame(ExceptionCode::COMPRESSION_OUTPUT_OVERFLOW->value, $e->getCode());
            $this->assertSame(
                [
                    'stage' => 'append_literals',
                    'outputLength' => 0,
                    'additionalOutputLength' => 2,
                    'expectedUncompressedLength' => 1,
                ],
                $e->getContext()
            );
        }
    }

    public function testDecompressBlockRejectsMatchBeyondExpectedLengthBeforeExpansion(): void {
        // One literal followed by a 261,139-byte RLE match. The raw block is
        // only about 1 KiB, but used to allocate output 261,140 times larger
        // than the length declared by the enclosing frame before its caller
        // could detect the mismatch.
        $extendedMatchLength = str_repeat("\xff", 1024) . "\x00";
        $input = "\x1fA\x01\x00" . $extendedMatchLength;
        $dec = new Lz4Decompressor($input, preferExtension: false);

        try {
            $dec->decompressBlock(1);
            $this->fail('expected a match beyond the declared output length to be refused');
        } catch (CompressionException $e) {
            $this->assertSame(ExceptionCode::COMPRESSION_OUTPUT_OVERFLOW->value, $e->getCode());
            $this->assertSame(
                [
                    'stage' => 'expand_match',
                    'outputLength' => 1,
                    'additionalOutputLength' => 261139,
                    'expectedUncompressedLength' => 1,
                ],
                $e->getContext()
            );
        }
    }

    public function testDecompressBlockRejectsNegativeExpectedLength(): void {
        $dec = new Lz4Decompressor("\x00", preferExtension: false);

        try {
            $dec->decompressBlock(-1);
            $this->fail('expected a negative output length to be refused');
        } catch (CompressionException $e) {
            $this->assertSame(ExceptionCode::COMPRESSION_ILLEGAL_VALUE->value, $e->getCode());
            $this->assertSame(
                [
                    'stage' => 'validate_expected_output_length',
                    'expectedUncompressedLength' => -1,
                ],
                $e->getContext()
            );
        }
    }

    public function testDecompressBlockThrowsOnIllegalOffset(): void {
        // Have some literals, then offset=0 -> illegal
        $token = chr((1 << 4) | 0x1); // literals=1, match nibble=1 (but we'll fail before using it)
        $literals = 'a';
        $offset = chr(0x00) . chr(0x00); // illegal offset 0
        $input = $token . $literals . $offset;

        $dec = new Lz4Decompressor($input);
        $this->expectException(CompressionException::class);
        $this->expectExceptionCode(ExceptionCode::COMPRESSION_ILLEGAL_VALUE->value);
        $dec->decompressBlock();
    }

    public function testDecompressBlockThrowsOnInputOverflowReadingLiteralsLength(): void {
        // token indicates extended literals length, but no extra byte present
        $token = chr((0x0F << 4) | 0x0);
        $input = $token; // missing extension byte

        $dec = new Lz4Decompressor($input);
        $this->expectException(CompressionException::class);
        $this->expectExceptionCode(ExceptionCode::COMPRESSION_INPUT_OVERFLOW->value);
        $dec->decompressBlock();
    }

    public function testDecompressBlockThrowsOnOutputUnderflow(): void {
        // No literals, try to reference offset larger than current output
        $token = chr((0 << 4) | 0x1);
        $offset = chr(0x02) . chr(0x00); // offset 2 but output length 0
        $input = $token . $offset;

        $dec = new Lz4Decompressor($input);
        $this->expectException(CompressionException::class);
        $this->expectExceptionCode(ExceptionCode::COMPRESSION_OUTPUT_UNDERFLOW->value);
        $dec->decompressBlock();
    }

    public function testDecompressBlockWithExtendedLiteralsLength(): void {
        // literals length uses 0xF then one extension byte 0x05 => total 0xF + 0x05 = 20
        $extended = 0x0F; // indicates extension
        $extByte = 0x05; // no more 0xFF continuation, so +5
        $token = chr(($extended << 4) | 0x0);
        $literals = str_repeat('x', 20);
        $input = $token . chr($extByte) . $literals;

        $dec = new Lz4Decompressor($input);
        $result = $dec->decompressBlock();
        $this->assertSame($literals, $result);
    }

    public function testDecompressBlockWithExtendedMatchLength(): void {
        // Start with 4 literals 'abcd', then match offset=4, match length base 0xF -> extended by 0x03 => 15 + 3 + 4 = 22
        // token: literals=4 (0x4), match nibble=0xF
        $token = chr((4 << 4) | 0x0F);
        $literals = 'abcd';
        $offset = chr(0x04) . chr(0x00);
        $ext = chr(0x03); // one extension byte 3
        $input = $token . $literals . $offset . $ext;

        $expected = 'abcd' . substr(str_repeat('abcd', 6), 0, 22); // 22 bytes copied from history
        $dec = new Lz4Decompressor($input, preferExtension: false);
        $result = $dec->decompressBlock(strlen($expected));
        $this->assertSame($expected, $result);
    }

    public function testDecompressBlockWithMatchCopy(): void {
        // Build block that produces "abcabcabca"
        // literals: "abc" (len=3)
        // match: offset=3, match length=(7+4)=11? We want 7, so low nibble=3 gives 3+4=7
        $token1 = chr((3 << 4) | 0x3); // literals=3, matchLenNibble=3 -> match length 7
        $literals1 = 'abc';
        $offset1 = chr(0x03) . chr(0x00); // little-endian 3
        // After copying 7 bytes from start (index 0..6): "abc" + copy("abcabca") => "abcabcabca"
        $input = $token1 . $literals1 . $offset1;

        $dec = new Lz4Decompressor($input);
        $result = $dec->decompressBlock();
        $this->assertSame('abcabcabca', $result);
    }

    public function testDecompressBlockWithOnlyLiterals(): void {
        // token: high nibble (literals length)=5, low nibble (match length)=0
        $token = chr((5 << 4) | 0x0);
        $literals = 'hello';
        $input = $token . $literals; // no offset -> end of input ends block

        $dec = new Lz4Decompressor($input);
        $result = $dec->decompressBlock();
        $this->assertSame('hello', $result);
    }

    public function testDecompressLegacyFrameWithoutABlockIsRefused(): void {
        $this->expectException(CompressionException::class);
        $this->expectExceptionCode(ExceptionCode::COMPRESSION_INPUT_OVERFLOW->value);

        (new Lz4Decompressor(pack('V', self::MAGIC_LEGACY)))->decompress();
    }

    public function testDecompressLegacyFrameWithSeveralBlocks(): void {
        // A legacy frame carries a sequence of blocks with neither a length nor
        // an end mark to say how many. Reading only the first and then looking
        // for the next frame's magic number took the second block's size field
        // for one and refused the rest of the input.
        $first = str_repeat('alpha-', 500);
        $second = str_repeat('bravo-', 500);

        $this->assertSame(
            $first . $second,
            (new Lz4Decompressor(self::legacyFrame([$first, $second])))->decompress()
        );

        // One block still works, and so does a second frame after the first —
        // the magic number that ends a frame must be left for decompress().
        $this->assertSame(
            $first,
            (new Lz4Decompressor(self::legacyFrame([$first])))->decompress()
        );

        $this->assertSame(
            $first . $second,
            (new Lz4Decompressor(self::legacyFrame([$first]) . self::legacyFrame([$second])))->decompress()
        );
    }

    public function testDecompressLegacyFrameWithTruncatedBlockIsRefused(): void {
        $this->expectException(CompressionException::class);
        $this->expectExceptionCode(ExceptionCode::COMPRESSION_INPUT_OVERFLOW->value);

        (new Lz4Decompressor(pack('V', self::MAGIC_LEGACY) . pack('V', 9999) . 'short'))->decompress();
    }

    public function testDependentFrameBlocksCanReferenceAnEarlierBlock(): void {
        $this->assertSame(
            'AAAAA',
            (new Lz4Decompressor(
                self::versionOneFrame(0x40, 0x40, [["\x10A", false], ["\x00\x01\x00", false]]),
                preferExtension: false,
            ))->decompress(),
        );
    }

    public function testIndependentFrameBlocksCannotReferenceAnEarlierBlock(): void {
        $this->expectException(CompressionException::class);
        $this->expectExceptionCode(ExceptionCode::COMPRESSION_OUTPUT_UNDERFLOW->value);

        (new Lz4Decompressor(
            self::versionOneFrame(0x60, 0x40, [["\x10A", false], ["\x00\x01\x00", false]]),
            preferExtension: false,
        ))->decompress();
    }

    public function testSetInputCutsALengthPastTheEndOfTheStringBackToIt(): void {
        // A length reaching past the string used to leave the decoder reading
        // off the end of it, which is a PHP warning and a zero byte rather than
        // the truncated input it is.
        $plain = str_repeat('hello world ', 20);
        $block = (new Lz4Compressor(false))->compressBlock($plain);

        $decompressor = new Lz4Decompressor(preferExtension: false);
        $decompressor->setInput(substr($block, 0, -1), 0, strlen($block));

        $this->expectException(CompressionException::class);
        $this->expectExceptionCode(ExceptionCode::COMPRESSION_INPUT_OVERFLOW->value);

        $decompressor->decompressBlock(strlen($plain));
    }

    public function testSetInputRejectsANegativeOffsetWithAProjectException(): void {
        try {
            new Lz4Decompressor('abcd', -1, preferExtension: false);
            $this->fail('expected a negative input offset to be refused');
        } catch (CompressionException $e) {
            $this->assertSame(ExceptionCode::COMPRESSION_INVALID_INPUT_OFFSET->value, $e->getCode());
            $this->assertSame(
                [
                    'stage' => 'set_input',
                    'inputOffset' => -1,
                    'inputLength' => 0,
                    'dataLength' => 4,
                ],
                $e->getContext(),
            );
        }
    }

    public function testSetInputRejectsAnOffsetPastTheInput(): void {
        $this->expectException(CompressionException::class);
        $this->expectExceptionCode(ExceptionCode::COMPRESSION_INVALID_INPUT_OFFSET->value);

        (new Lz4Decompressor(preferExtension: false))->setInput('abcd', 5);
    }

    public function testSetInputTakesALengthCountedFromTheOffset(): void {
        // The regression this exists for: the length was used as an absolute
        // end offset, so a block preceded by anything at all was cut short by
        // exactly the offset it started at.
        $plain = str_repeat('hello world ', 20);
        $block = (new Lz4Compressor(false))->compressBlock($plain);
        $prefix = 'XXXXX';

        $decompressor = new Lz4Decompressor(preferExtension: false);
        $decompressor->setInput($prefix . $block, strlen($prefix), strlen($block));

        $this->assertSame($plain, $decompressor->decompressBlock(strlen($plain)));

        $viaConstructor = new Lz4Decompressor(
            $prefix . $block,
            strlen($prefix),
            strlen($block),
            preferExtension: false,
        );

        $this->assertSame($plain, $viaConstructor->decompressBlock(strlen($plain)));
    }

    public function testVersionOneFrameAcceptsAMatchingContentSize(): void {
        $this->assertSame(
            'abc',
            (new Lz4Decompressor(
                self::versionOneFrame(0x68, 0x40, [['abc', true]], contentSize: 3),
                preferExtension: false,
            ))->decompress(),
        );
    }

    public function testVersionOneFrameBoundsCompressedBlockOutputByItsDeclaredMaximum(): void {
        $extendedMatchLength = str_repeat("\xff", 300) . "\x00";
        $block = "\x1fA\x01\x00" . $extendedMatchLength;

        try {
            (new Lz4Decompressor(
                self::versionOneFrame(0x60, 0x40, [[$block, false]]),
                preferExtension: false,
            ))->decompress();
            $this->fail('expected decompressed block output past the declared maximum to be refused');
        } catch (CompressionException $e) {
            $this->assertSame(ExceptionCode::COMPRESSION_FRAME_BLOCK_TOO_LARGE->value, $e->getCode());
            $this->assertInstanceOf(CompressionException::class, $e->getPrevious());
            $this->assertSame(ExceptionCode::COMPRESSION_OUTPUT_OVERFLOW->value, $e->getPrevious()->getCode());
        }
    }

    public function testVersionOneFrameRejectsAMismatchedContentSize(): void {
        $this->expectException(CompressionException::class);
        $this->expectExceptionCode(ExceptionCode::COMPRESSION_CONTENT_SIZE_MISMATCH->value);

        (new Lz4Decompressor(
            self::versionOneFrame(0x68, 0x40, [['x', true]], contentSize: 0),
            preferExtension: false,
        ))->decompress();
    }

    public function testVersionOneFrameRejectsAStoredBlockPastItsDeclaredMaximum(): void {
        $this->expectException(CompressionException::class);
        $this->expectExceptionCode(ExceptionCode::COMPRESSION_FRAME_BLOCK_TOO_LARGE->value);

        (new Lz4Decompressor(
            self::versionOneFrame(0x60, 0x40, [[str_repeat('x', (64 * 1024) + 1), true]]),
            preferExtension: false,
        ))->decompress();
    }

    public function testVersionOneFrameRejectsDictionaryFrames(): void {
        $this->expectException(CompressionException::class);
        $this->expectExceptionCode(ExceptionCode::COMPRESSION_UNSUPPORTED_DICTIONARY->value);

        (new Lz4Decompressor(self::versionOneFrame(0x61, 0x40, [['x', true]]), preferExtension: false))->decompress();
    }

    /**
     * @dataProvider invalidFrameDescriptorProvider
     *
     * @param int<0, 255> $flg
     * @param int<0, 255> $bd
     */
    public function testVersionOneFrameRejectsInvalidDescriptorBits(int $flg, int $bd): void {
        $this->expectException(CompressionException::class);
        $this->expectExceptionCode(ExceptionCode::COMPRESSION_INVALID_FRAME_DESCRIPTOR->value);

        (new Lz4Decompressor(self::versionOneFrame($flg, $bd, [['x', true]]), preferExtension: false))->decompress();
    }

    /**
     * A pre-1.4 "legacy" frame: the magic number, then one length-prefixed raw
     * LZ4 block per entry.
     *
     * @param array<string> $blocks the uncompressed content of each block
     *
     * @throws \Cassandra\Exception\CompressionException
     */
    private static function legacyFrame(array $blocks): string {

        $compressor = new Lz4Compressor();

        $frame = pack('V', self::MAGIC_LEGACY);

        foreach ($blocks as $block) {
            $compressed = $compressor->compressBlock($block);
            $frame .= pack('V', strlen($compressed)) . $compressed;
        }

        return $frame;
    }

    /**
     * @param list<array{string, bool}> $blocks block data and whether it is stored uncompressed
     * @param int<0, 255> $flg
     * @param int<0, 255> $bd
     */
    private static function versionOneFrame(int $flg, int $bd, array $blocks, int $contentSize = 0): string {
        $descriptor = chr($flg) . chr($bd);
        if (($flg & 0x08) !== 0) {
            $descriptor .= pack('V2', $contentSize, 0);
        }
        if (($flg & 0x01) !== 0) {
            $descriptor .= pack('V', 1);
        }

        $headerChecksum = ord(hash('xxh32', $descriptor, true, ['seed' => 0])[2]);
        $frame = pack('V', self::MAGIC_V1) . $descriptor . chr($headerChecksum);

        foreach ($blocks as [$block, $isUncompressed]) {
            $blockSize = pack('V', strlen($block));
            if ($isUncompressed) {
                $blockSize[3] = chr((ord($blockSize[3]) | 0x80) & 0xFF);
            }
            $frame .= $blockSize . $block;
        }

        return $frame . pack('V', 0);
    }
}
