<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\StringUtil;

class StringUtilTest extends AbstractUnitTestCase {
    /**
     * @return array<array{0: string, 1: bool}>
     */
    public static function providerIsDigits(): array {
        return [
            ['', false],
            ['0', true],
            ['9', true],
            ['0123456789', true],
            [str_repeat('0', 100), true],
            [str_repeat('7', 100), true],
            ['12a', false],
            ['a12', false],
            ['1a2', false],
            [' 12', false],
            ['12 ', false],
            ["12\n", false],
            ["12\x00", false],
            ["\x0012", false],
            ['+1', false],
            ['-1', false],
            ['1.5', false],
            ['1e5', false],
            ['/', false],
            [':', false],
            ["\xff", false],
            // full-width digits must not be accepted
            ['１２３', false],
        ];
    }

    /**
     * @return array<array{0: string, 1: bool}>
     */
    public static function providerIsHexDigits(): array {
        return [
            ['', false],
            ['0', true],
            ['9', true],
            ['a', true],
            ['f', true],
            ['A', true],
            ['F', true],
            ['0123456789abcdefABCDEF', true],
            [str_repeat('ab', 50), true],
            ['g', false],
            ['G', false],
            ['0g', false],
            ['g0', false],
            ['0f ', false],
            [' 0f', false],
            ["0f\x00", false],
            ['0x1f', false],
            ['-1f', false],
            ['/', false],
            [':', false],
            ['@', false],
            ['`', false],
            ["\xff", false],
            ['ｆ', false],
        ];
    }

    /**
     * @dataProvider providerIsDigits
     */
    public function testIsDigits(string $value, bool $expected): void {
        $this->assertSame($expected, StringUtil::isDigits($value));
    }

    /**
     * isDigits() replaced ctype_digit(); the two must agree on every single byte.
     */
    public function testIsDigitsMatchesCtypeDigitForAllBytes(): void {

        if (!extension_loaded('ctype')) {
            $this->markTestSkipped('ext-ctype is not available; the library no longer requires it.');
        }

        for ($byte = 0; $byte < 256; $byte++) {
            $char = chr($byte);
            $this->assertSame(
                ctype_digit($char),
                StringUtil::isDigits($char),
                sprintf('byte 0x%02X', $byte)
            );
        }
    }

    /**
     * @dataProvider providerIsHexDigits
     */
    public function testIsHexDigits(string $value, bool $expected): void {
        $this->assertSame($expected, StringUtil::isHexDigits($value));
    }

    /**
     * isHexDigits() replaced ctype_xdigit(); the two must agree on every single byte.
     */
    public function testIsHexDigitsMatchesCtypeXdigitForAllBytes(): void {

        if (!extension_loaded('ctype')) {
            $this->markTestSkipped('ext-ctype is not available; the library no longer requires it.');
        }

        for ($byte = 0; $byte < 256; $byte++) {
            $char = chr($byte);
            $this->assertSame(
                ctype_xdigit($char),
                StringUtil::isHexDigits($char),
                sprintf('byte 0x%02X', $byte)
            );
        }
    }
}
