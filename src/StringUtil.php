<?php

declare(strict_types=1);

namespace Cassandra;

/**
 * ASCII character-class checks used for validating numeric and hex strings.
 *
 * These intentionally avoid ext-ctype so the library needs no PHP extension.
 * strspn() stays roughly flat as the subject grows, whereas ctype_digit()/
 * ctype_xdigit() cost scales per character; including the call overhead of these
 * wrappers the two break even at roughly 40 characters, so strspn() is the better
 * fit for the typically long strings checked here (varint/decimal digit strings,
 * hex-encoded type names). It is also not locale-sensitive.
 *
 * Note that these return false for the empty string, matching ctype_digit('') /
 * ctype_xdigit('') — strspn() alone would report an empty string as valid.
 */
final class StringUtil {
    private const DIGITS = '0123456789';
    private const HEX_DIGITS = '0123456789ABCDEFabcdef';

    /**
     * Returns true if $value is a non-empty string of ASCII decimal digits only.
     *
     * @phpstan-assert-if-true numeric-string $value
     * @psalm-assert-if-true numeric-string $value
     */
    public static function isDigits(string $value): bool {
        return $value !== '' && strspn($value, self::DIGITS) === strlen($value);
    }

    /**
     * Returns true if $value is a non-empty string of ASCII hex digits only.
     */
    public static function isHexDigits(string $value): bool {
        return $value !== '' && strspn($value, self::HEX_DIGITS) === strlen($value);
    }
}
