# StreamReader

***

* Full name: `\Cassandra\Response\StreamReader`

## Methods

### __construct

```php
public __construct(string $data): mixed
```

**Parameters:**

| Parameter | Type       | Description |
|-----------|------------|-------------|
| `$data`   | **string** |             |

***

### extraDataOffset

Sets the extra data offset used to hide extra data at the beginning of the response.

```php
public extraDataOffset(int $extraDataOffset): void
```

**Parameters:**

| Parameter          | Type    | Description |
|--------------------|---------|-------------|
| `$extraDataOffset` | **int** |             |

***

### getData

```php
public getData(bool $includeExtraData = false): string
```

**Parameters:**

| Parameter           | Type     | Description |
|---------------------|----------|-------------|
| `$includeExtraData` | **bool** |             |

***

### offset

```php
public offset(int $offset): void
```

**Parameters:**

| Parameter | Type    | Description |
|-----------|---------|-------------|
| `$offset` | **int** |             |

***

### pos

```php
public pos(): int
```

***

### read

```php
public read(int $length): string
```

**Parameters:**

| Parameter | Type    | Description |
|-----------|---------|-------------|
| `$length` | **int** |             |

**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

***

### readByte

Reads a 1 byte unsigned integer

```php
final public readByte(): int
```

* This method is **final**.
**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

***

### readBytes

```php
final public readBytes(): ?string
```

* This method is **final**.
**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

***

### readBytesMap

```php
final public readBytesMap(): array<string,?string>
```

* This method is **final**.
**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

***

### readConsistency

```php
final public readConsistency(): \Cassandra\Consistency
```

* This method is **final**.
**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

***

### readDouble

Reads an IEEE-754 big-endian double (8 bytes).

```php
final public readDouble(): float
```

* This method is **final**.
**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

***

### readFloat

Reads an IEEE-754 big-endian float (4 bytes).

```php
final public readFloat(): float
```

* This method is **final**.
**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

***

### readInet

```php
final public readInet(): array{ip: string, port: int}
```

* This method is **final**.
**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

***

### readInetAddr

```php
final public readInetAddr(): string
```

* This method is **final**.
**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

***

### readInt

```php
final public readInt(): int
```

* This method is **final**.
**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

***

### readLong

```php
final public readLong(): int
```

* This method is **final**.
**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

***

### readLongString

```php
final public readLongString(): string
```

* This method is **final**.
**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

***

### readReasonMap

```php
final public readReasonMap(): array<string,int>
```

* This method is **final**.
**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

***

### readShort

```php
final public readShort(): int
```

* This method is **final**.
**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

***

### readShortBytes

```php
final public readShortBytes(): string
```

* This method is **final**.
**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

***

### readSignedVint32

Reads a signed VInt with a maximum size of 32 bits

```php
final public readSignedVint32(): int
```

* This method is **final**.
**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)
- [`VIntCodecException`](../Exception/VIntCodecException.md)

***

### readSignedVint64

Reads a signed VInt with a maximum size of 64 bits.

```php
final public readSignedVint64(): int
```

This is named "vint" in the native protocol specification.

* This method is **final**.
**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

***

### readString

```php
final public readString(): string
```

* This method is **final**.
**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

***

### readStringList

```php
final public readStringList(): string[]
```

* This method is **final**.
**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

***

### readStringMap

```php
final public readStringMap(): array<string,string>
```

* This method is **final**.
**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

***

### readStringMultimap

```php
final public readStringMultimap(): array<string,string[]>
```

* This method is **final**.
**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

***

### readTypeInfo

Reads a type info object.

```php
final public readTypeInfo(): \Cassandra\TypeInfo\TypeInfo
```

The native protocol specification calls this an "option".

* This method is **final**.
**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)
- [`ValueException`](../Exception/ValueException.md)
- [`TypeNameParserException`](../Exception/TypeNameParserException.md)

***

### readUnsignedVInt32

Reads an unsigned VInt with a maximum size of 32 bits

```php
final public readUnsignedVInt32(): int
```

* This method is **final**.
**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)
- [`VIntCodecException`](../Exception/VIntCodecException.md)

***

### readUnsignedVInt64

Reads an unsigned VInt with a maximum size of 64 bits.

```php
final public readUnsignedVInt64(): int
```

This is named "unsigned vint" in the native protocol specification.

* This method is **final**.
**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

***

### readUuid

```php
final public readUuid(): string
```

* This method is **final**.
**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

***

### readValue

```php
final public readValue(\Cassandra\TypeInfo\TypeInfo $typeInfo, \Cassandra\Value\ValueEncodeConfig $valueEncodeConfig): mixed
```

* This method is **final**.
**Parameters:**

| Parameter            | Type                                   | Description |
|----------------------|----------------------------------------|-------------|
| `$typeInfo`          | **\Cassandra\TypeInfo\TypeInfo**       |             |
| `$valueEncodeConfig` | **\Cassandra\Value\ValueEncodeConfig** |             |

**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)
- [`ValueException`](../Exception/ValueException.md)
- [`ValueFactoryException`](../Exception/ValueFactoryException.md)

***

### reset

```php
public reset(): void
```

***
