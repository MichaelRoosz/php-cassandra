# Uuid

***

* Full name: `\Cassandra\Value\Uuid`
* Parent class: [`\Cassandra\Value\ValueWithFixedLength`](./ValueWithFixedLength.md)
* This class is marked as **final** and can't be subclassed
* This class implements:
  [`\Cassandra\Value\ValueWithMultipleEncodings`](./ValueWithMultipleEncodings.md)
* This class is a **Final class**

## Methods

### __construct

Accepts the canonical 36-character string form
(xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx), the compact 32-character undashed
hex form (xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx), or the raw 16-byte binary form
(for example the value returned by a UuidEncodeOption::AS_BINARY read). The
three forms are distinguished by length and cannot collide (16, 32 and 36
bytes respectively). The value is stored raw, so getBinary() is a no-op and
the canonical string form is produced on demand.

```php
final public __construct(string $value): mixed
```

* This method is **final**.
**Parameters:**

| Parameter | Type       | Description |
|-----------|------------|-------------|
| `$value`  | **string** |             |

**Throws:**

- [`ValueException`](../Exception/ValueException.md)

***

### asConfigured

```php
public asConfigured(\Cassandra\Value\ValueEncodeConfig $valueEncodeConfig): mixed
```

**Parameters:**

| Parameter            | Type                                   | Description |
|----------------------|----------------------------------------|-------------|
| `$valueEncodeConfig` | **\Cassandra\Value\ValueEncodeConfig** |             |

***

### fixedLength

```php
final public static fixedLength(): int
```

* This method is **static**.* This method is **final**.
***

### fromBinary

```php
public static fromBinary(string $binary, ?\Cassandra\TypeInfo\TypeInfo $typeInfo = null, ?\Cassandra\Value\ValueEncodeConfig $valueEncodeConfig = null): static
```

* This method is **static**.
**Parameters:**

| Parameter            | Type                                    | Description |
|----------------------|-----------------------------------------|-------------|
| `$binary`            | **string**                              |             |
| `$typeInfo`          | **?\Cassandra\TypeInfo\TypeInfo**       |             |
| `$valueEncodeConfig` | **?\Cassandra\Value\ValueEncodeConfig** |             |

**Throws:**

- [`ValueException`](../Exception/ValueException.md)

***

### fromMixedValue

```php
public static fromMixedValue(mixed $value, ?\Cassandra\TypeInfo\TypeInfo $typeInfo = null): static
```

* This method is **static**.
**Parameters:**

| Parameter   | Type                              | Description |
|-------------|-----------------------------------|-------------|
| `$value`    | **mixed**                         |             |
| `$typeInfo` | **?\Cassandra\TypeInfo\TypeInfo** |             |

**Throws:**

- [`ValueException`](../Exception/ValueException.md)

***

### fromValue

```php
final public static fromValue(string $value): static
```

* This method is **static**.* This method is **final**.
**Parameters:**

| Parameter | Type       | Description |
|-----------|------------|-------------|
| `$value`  | **string** |             |

**Throws:**

- [`ValueException`](../Exception/ValueException.md)

***

### getBinary

```php
public getBinary(): string
```

***

### getType

```php
public getType(): \Cassandra\Type
```

***

### getValue

```php
public getValue(): string
```

***

### isSerializedAsFixedLength

```php
final public static isSerializedAsFixedLength(): bool
```

* This method is **static**.* This method is **final**.
***

### random

```php
public static random(): static
```

* This method is **static**.
**Throws:**

- [`ValueException`](../Exception/ValueException.md)

***

### requiresDefinition

```php
final public static requiresDefinition(): bool
```

* This method is **static**.* This method is **final**.
***

## Inherited methods

### __toString

```php
public __toString(): string
```

**Throws:**

- [`ValueException`](../Exception/ValueException.md)

***

### fixedLength

```php
public static fixedLength(): int
```

* This method is **static**.* This method is **abstract**.
***

### fromBinary

```php
public static fromBinary(string $binary, ?\Cassandra\TypeInfo\TypeInfo $typeInfo = null, ?\Cassandra\Value\ValueEncodeConfig $valueEncodeConfig = null): static
```

* This method is **static**.* This method is **abstract**.
**Parameters:**

| Parameter            | Type                                    | Description |
|----------------------|-----------------------------------------|-------------|
| `$binary`            | **string**                              |             |
| `$typeInfo`          | **?\Cassandra\TypeInfo\TypeInfo**       |             |
| `$valueEncodeConfig` | **?\Cassandra\Value\ValueEncodeConfig** |             |

**Throws:**

- [`ValueException`](../Exception/ValueException.md)
- [`ValueFactoryException`](../Exception/ValueFactoryException.md)

***

### fromMixedValue

```php
public static fromMixedValue(mixed $value, ?\Cassandra\TypeInfo\TypeInfo $typeInfo = null): static
```

* This method is **static**.* This method is **abstract**.
**Parameters:**

| Parameter   | Type                              | Description |
|-------------|-----------------------------------|-------------|
| `$value`    | **mixed**                         |             |
| `$typeInfo` | **?\Cassandra\TypeInfo\TypeInfo** |             |

**Throws:**

- [`ValueException`](../Exception/ValueException.md)
- [`ValueFactoryException`](../Exception/ValueFactoryException.md)

***

### fromStream

```php
public static fromStream(\Cassandra\Response\StreamReader $stream, ?int $length = null, ?\Cassandra\TypeInfo\TypeInfo $typeInfo = null, ?\Cassandra\Value\ValueEncodeConfig $valueEncodeConfig = null): static
```

* This method is **static**.
**Parameters:**

| Parameter            | Type                                    | Description |
|----------------------|-----------------------------------------|-------------|
| `$stream`            | **\Cassandra\Response\StreamReader**    |             |
| `$length`            | **?int**                                |             |
| `$typeInfo`          | **?\Cassandra\TypeInfo\TypeInfo**       |             |
| `$valueEncodeConfig` | **?\Cassandra\Value\ValueEncodeConfig** |             |

**Throws:**

- [`ValueException`](../Exception/ValueException.md)
- [`ValueFactoryException`](../Exception/ValueFactoryException.md)
- [`ResponseException`](../Exception/ResponseException.md)

***

### getBinary

```php
public getBinary(): string
```

* This method is **abstract**.
***

### getType

```php
public getType(): \Cassandra\Type
```

* This method is **abstract**.
***

### getValue

```php
public getValue(): mixed
```

* This method is **abstract**.
***

### hasFixedLength

```php
final public static hasFixedLength(): bool
```

* This method is **static**.* This method is **final**.
***

### isReadableWithoutLength

```php
final public static isReadableWithoutLength(): bool
```

* This method is **static**.* This method is **final**.
***

### isSerializedAsFixedLength

```php
public static isSerializedAsFixedLength(): bool
```

* This method is **static**.* This method is **abstract**.
***

### requiresDefinition

```php
public static requiresDefinition(): bool
```

* This method is **static**.* This method is **abstract**.
***
