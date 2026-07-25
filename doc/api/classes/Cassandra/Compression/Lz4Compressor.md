# Lz4Compressor

Pure-PHP LZ4 block compressor.

Produces a raw LZ4 block (no frame header/footer) that is compatible with


- **See:** \Cassandra\Compression\Lz4Decompressor::decompressBlock() and with the block payload the
CQL binary protocol expects for LZ4 compression.

The implementation uses a single fixed-size hash table and a greedy match
search (the classic "LZ4_compress_fast" strategy). It intentionally trades a
little compression ratio for speed, which matters because everything here
runs in plain PHP.

***

* Full name: `\Cassandra\Compression\Lz4Compressor`
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Methods

### __construct

```php
public __construct(bool $preferExtension = true): mixed
```

**Parameters:**

| Parameter          | Type     | Description                                                                                                                                                                |
|--------------------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `$preferExtension` | **bool** | Use the native LZ4 PHP extension when it is
available (much faster). Pass false to force the pure-PHP implementation,
e.g. in tests that must exercise this code directly. |

***

### compress

Compress a string into a complete, standards-compliant LZ4 frame (spec
v1.6.x, magic 0x184D2204) that any conforming LZ4 tool can decode — the
counterpart to {@see Lz4Decompressor::decompress()}.

```php
public compress(string $input): string
```

The frame uses block-independent 4 MiB blocks with no block or content
checksums. Each block is produced by 

- **See:** \Cassandra\Compression\compressBlock() (so it also
benefits from the native extension when available); a block that would not
shrink is stored uncompressed.

**Parameters:**

| Parameter | Type       | Description |
|-----------|------------|-------------|
| `$input`  | **string** |             |

***

### compressBlock

Compress a string into a single raw LZ4 block.

```php
public compressBlock(string $input): string
```

**Parameters:**

| Parameter | Type       | Description |
|-----------|------------|-------------|
| `$input`  | **string** |             |

***
