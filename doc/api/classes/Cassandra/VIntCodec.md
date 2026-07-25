# VIntCodec

Note that a VInt is different from a Varint.

See native_protocol_v5.spec for more details.

***

* Full name: `\Cassandra\VIntCodec`
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Methods

### decodeSignedVint32

```php
final public decodeSignedVint32(string $binary): int
```

* This method is **final**.
**Parameters:**

| Parameter | Type       | Description |
|-----------|------------|-------------|
| `$binary` | **string** |             |

**Throws:**

- [`VIntCodecException`](./Exception/VIntCodecException.md)

***

### decodeSignedVint64

```php
final public decodeSignedVint64(string $binary): int
```

* This method is **final**.
**Parameters:**

| Parameter | Type       | Description |
|-----------|------------|-------------|
| `$binary` | **string** |             |

**Throws:**

- [`VIntCodecException`](./Exception/VIntCodecException.md)

***

### decodeUnsignedVint32

```php
final public decodeUnsignedVint32(string $binary): int
```

* This method is **final**.
**Parameters:**

| Parameter | Type       | Description |
|-----------|------------|-------------|
| `$binary` | **string** |             |

**Throws:**

- [`VIntCodecException`](./Exception/VIntCodecException.md)

***

### decodeUnsignedVint64

```php
final public decodeUnsignedVint64(string $binary): int
```

* This method is **final**.
**Parameters:**

| Parameter | Type       | Description |
|-----------|------------|-------------|
| `$binary` | **string** |             |

**Throws:**

- [`VIntCodecException`](./Exception/VIntCodecException.md)

***

### encodeSignedVint32

```php
final public encodeSignedVint32(int $number): string
```

* This method is **final**.
**Parameters:**

| Parameter | Type    | Description |
|-----------|---------|-------------|
| `$number` | **int** |             |

**Throws:**

- [`VIntCodecException`](./Exception/VIntCodecException.md)

***

### encodeSignedVint64

```php
final public encodeSignedVint64(int $number): string
```

* This method is **final**.
**Parameters:**

| Parameter | Type    | Description |
|-----------|---------|-------------|
| `$number` | **int** |             |

***

### encodeUnsignedVint32

```php
final public encodeUnsignedVint32(int $number): string
```

* This method is **final**.
**Parameters:**

| Parameter | Type    | Description |
|-----------|---------|-------------|
| `$number` | **int** |             |

**Throws:**

- [`VIntCodecException`](./Exception/VIntCodecException.md)

***

### encodeUnsignedVint64

```php
final public encodeUnsignedVint64(int $number): string
```

* This method is **final**.
**Parameters:**

| Parameter | Type    | Description |
|-----------|---------|-------------|
| `$number` | **int** |             |

***

### readSignedVint32

```php
final public readSignedVint32(\Cassandra\Response\StreamReader $stream): int
```

* This method is **final**.
**Parameters:**

| Parameter | Type                                 | Description |
|-----------|--------------------------------------|-------------|
| `$stream` | **\Cassandra\Response\StreamReader** |             |

**Throws:**

- [`VIntCodecException`](./Exception/VIntCodecException.md)
- [`ResponseException`](./Exception/ResponseException.md)

***

### readSignedVint64

```php
final public readSignedVint64(\Cassandra\Response\StreamReader $stream): int
```

* This method is **final**.
**Parameters:**

| Parameter | Type                                 | Description |
|-----------|--------------------------------------|-------------|
| `$stream` | **\Cassandra\Response\StreamReader** |             |

**Throws:**

- [`ResponseException`](./Exception/ResponseException.md)

***

### readUnsignedVint32

```php
final public readUnsignedVint32(\Cassandra\Response\StreamReader $stream): int
```

* This method is **final**.
**Parameters:**

| Parameter | Type                                 | Description |
|-----------|--------------------------------------|-------------|
| `$stream` | **\Cassandra\Response\StreamReader** |             |

**Throws:**

- [`VIntCodecException`](./Exception/VIntCodecException.md)
- [`ResponseException`](./Exception/ResponseException.md)

***

### readUnsignedVint64

```php
final public readUnsignedVint64(\Cassandra\Response\StreamReader $stream): int
```

* This method is **final**.
**Parameters:**

| Parameter | Type                                 | Description |
|-----------|--------------------------------------|-------------|
| `$stream` | **\Cassandra\Response\StreamReader** |             |

**Throws:**

- [`ResponseException`](./Exception/ResponseException.md)

***

### zigZagDecode

```php
final public zigZagDecode(int $number): int
```

* This method is **final**.
**Parameters:**

| Parameter | Type    | Description |
|-----------|---------|-------------|
| `$number` | **int** |             |

***

### zigZagEncode

```php
final public zigZagEncode(int $number): int
```

* This method is **final**.
**Parameters:**

| Parameter | Type    | Description |
|-----------|---------|-------------|
| `$number` | **int** |             |

***
