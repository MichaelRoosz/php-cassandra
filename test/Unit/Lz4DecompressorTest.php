<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Compression\Exception as CompressionException;
use Cassandra\Compression\Lz4Decompressor;
use Cassandra\ExceptionCode;
use PHPUnit\Framework\TestCase;

class Lz4DecompressorTest extends TestCase {
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

        $dec = new Lz4Decompressor($input);
        $result = $dec->decompressBlock();
        $expected = 'abcd' . substr(str_repeat('abcd', 6), 0, 22); // 22 bytes copied from history
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
}
