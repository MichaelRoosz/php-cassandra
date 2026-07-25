# Lz4Extension

Thin adapter over the optional native LZ4 PHP extension (the `lz4` PECL
extension, e.g. kjdev/php-ext-lz4), used to accelerate raw LZ4 block
(de)compression when it is installed.

The extension's `lz4_compress()` output is a 4-byte little-endian uncompressed
length followed by a raw LZ4 block; the CQL binary protocol uses just the raw
block. So compression strips that 4-byte prefix, and decompression prepends it
(the uncompressed length is always known from the frame header / body prefix).

Availability is confirmed once, at first use, with a self-test that verifies
this exact framing so a future extension version with a different container
format can never silently corrupt data — it simply falls back to the pure-PHP
implementation.

***

* Full name: `\Cassandra\Compression\Lz4Extension`
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Methods

### compressBlock

Compress a string into a raw LZ4 block, or null if the extension is
unavailable or failed.

```php
public static compressBlock(string $input): ?string
```

* This method is **static**.
**Parameters:**

| Parameter | Type       | Description |
|-----------|------------|-------------|
| `$input`  | **string** |             |

***

### decompressBlock

Decompress a raw LZ4 block of known uncompressed length, or null if the
extension is unavailable or failed.

```php
public static decompressBlock(string $block, int $uncompressedLength): ?string
```

* This method is **static**.
**Parameters:**

| Parameter             | Type       | Description |
|-----------------------|------------|-------------|
| `$block`              | **string** |             |
| `$uncompressedLength` | **int**    |             |

***

### isAvailable

```php
public static isAvailable(): bool
```

* This method is **static**.
***
