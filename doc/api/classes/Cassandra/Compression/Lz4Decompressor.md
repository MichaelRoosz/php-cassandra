# Lz4Decompressor

***

* Full name: `\Cassandra\Compression\Lz4Decompressor`
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Methods

### __construct

```php
public __construct(?string $compressedData = null, int $inputOffset = 0, int $inputLength = 0, bool $preferExtension = true): mixed
```

**Parameters:**

| Parameter          | Type        | Description                                                                                                                                                                                                                             |
|--------------------|-------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `$compressedData`  | **?string** |                                                                                                                                                                                                                                         |
| `$inputOffset`     | **int**     |                                                                                                                                                                                                                                         |
| `$inputLength`     | **int**     |                                                                                                                                                                                                                                         |
| `$preferExtension` | **bool**    | Use the native LZ4 PHP extension when it is
available (much faster). Only applies to {@see \Cassandra\Compression\decompressBlock()} when an
expected uncompressed length is supplied; pass false to force the
pure-PHP implementation. |

***

### decompress

```php
public decompress(bool $validateChecksums = true): string
```

**Parameters:**

| Parameter            | Type     | Description |
|----------------------|----------|-------------|
| `$validateChecksums` | **bool** |             |

**Throws:**

- [`CompressionException`](../Exception/CompressionException.md)

***

### decompressBlock

Decompress the raw LZ4 block set via {@see setInput()}.

```php
public decompressBlock(?int $expectedUncompressedLength = null): string
```

When the caller knows the uncompressed length (it comes from the frame
header on v5 and the 4-byte body prefix on v3/v4) and the native LZ4
extension is available, decompression is delegated to it; otherwise the
pure-PHP decoder is used.

**Parameters:**

| Parameter                     | Type     | Description |
|-------------------------------|----------|-------------|
| `$expectedUncompressedLength` | **?int** |             |

**Throws:**

- [`CompressionException`](../Exception/CompressionException.md)

***

### setInput

```php
public setInput(string $compressedData, int $inputOffset = 0, int $inputLength = 0): void
```

**Parameters:**

| Parameter         | Type       | Description |
|-------------------|------------|-------------|
| `$compressedData` | **string** |             |
| `$inputOffset`    | **int**    |             |
| `$inputLength`    | **int**    |             |

***
