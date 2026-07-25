# Duration

***

* Full name: `\Cassandra\Value\Duration`
* Parent class: [`\Cassandra\Value\ValueReadableWithoutLength`](./ValueReadableWithoutLength.md)
* This class is marked as **final** and can't be subclassed
* This class implements:
  [`\Cassandra\Value\ValueWithMultipleEncodings`](./ValueWithMultipleEncodings.md)
* This class is a **Final class**

## Methods

### __construct

```php
final public __construct(array{months: int, days: int, nanoseconds: int}|string|\DateInterval $value): mixed
```

* This method is **final**.
**Parameters:**

| Parameter | Type                                                                       | Description |
|-----------|----------------------------------------------------------------------------|-------------|
| `$value`  | **array{months: int, days: int, nanoseconds: int}\|string\|\DateInterval** |             |

**Throws:**

- [`ValueException`](../Exception/ValueException.md)

***

### __toString

```php
public __toString(): string
```

***

### asConfigured

```php
public asConfigured(\Cassandra\Value\ValueEncodeConfig $valueEncodeConfig): mixed
```

**Parameters:**

| Parameter            | Type                                   | Description |
|----------------------|----------------------------------------|-------------|
| `$valueEncodeConfig` | **\Cassandra\Value\ValueEncodeConfig** |             |

**Throws:**

- [`ValueException`](../Exception/ValueException.md)

***

### asDateInterval

```php
public asDateInterval(): \DateInterval
```

**Throws:**

- [`ValueException`](../Exception/ValueException.md)

***

### asDateIntervalString

```php
public asDateIntervalString(): string
```

***

### asNativeValue

```php
public asNativeValue(): array{months: int, days: int, nanoseconds: int}
```

***

### asString

```php
public asString(): string
```

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
- [`ResponseException`](../Exception/ResponseException.md)
- [`VIntCodecException`](../Exception/VIntCodecException.md)

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

### fromStream

```php
final public static fromStream(\Cassandra\Response\StreamReader $stream, ?int $length = null, ?\Cassandra\TypeInfo\TypeInfo $typeInfo = null, ?\Cassandra\Value\ValueEncodeConfig $valueEncodeConfig = null): static
```

* This method is **static**.* This method is **final**.
**Parameters:**

| Parameter            | Type                                    | Description |
|----------------------|-----------------------------------------|-------------|
| `$stream`            | **\Cassandra\Response\StreamReader**    |             |
| `$length`            | **?int**                                |             |
| `$typeInfo`          | **?\Cassandra\TypeInfo\TypeInfo**       |             |
| `$valueEncodeConfig` | **?\Cassandra\Value\ValueEncodeConfig** |             |

**Throws:**

- [`ValueException`](../Exception/ValueException.md)
- [`ResponseException`](../Exception/ResponseException.md)
- [`VIntCodecException`](../Exception/VIntCodecException.md)

***

### fromValue

```php
final public static fromValue(array{months: int, days: int, nanoseconds: int}|string|\DateInterval $value): static
```

* This method is **static**.* This method is **final**.
**Parameters:**

| Parameter | Type                                                                       | Description |
|-----------|----------------------------------------------------------------------------|-------------|
| `$value`  | **array{months: int, days: int, nanoseconds: int}\|string\|\DateInterval** |             |

**Throws:**

- [`ValueException`](../Exception/ValueException.md)

***

### getBinary

```php
public getBinary(): string
```

**Throws:**

- [`VIntCodecException`](../Exception/VIntCodecException.md)

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
final public static fixedLength(): int
```

* This method is **static**.* This method is **final**.
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

* This method is **static**.* This method is **abstract**.
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
final public static isSerializedAsFixedLength(): bool
```

* This method is **static**.* This method is **final**.
***

### requiresDefinition

```php
public static requiresDefinition(): bool
```

* This method is **static**.* This method is **abstract**.
***
