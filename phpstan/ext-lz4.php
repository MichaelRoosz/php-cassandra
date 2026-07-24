<?php

declare(strict_types=1);

/*
 * Signature stubs for the optional native LZ4 PHP extension (the `lz4` PECL
 * extension, e.g. kjdev/php-ext-lz4), so static analysis can check the calls in
 * Cassandra\Compression\Lz4Extension even when the extension is not installed in
 * the analysis environment. Not loaded at runtime.
 */

if (!function_exists('lz4_compress')) {
    /**
     * @return string|false
     */
    function lz4_compress(string $data, int $level = 0, ?string $extra = null): string|false {
        return false;
    }
}

if (!function_exists('lz4_uncompress')) {
    /**
     * @return string|false
     */
    function lz4_uncompress(string $data, ?string $extra = null): string|false {
        return false;
    }
}
