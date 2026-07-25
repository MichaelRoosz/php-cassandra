# Native

***

* Full name: `\Cassandra\StringMath\DecimalCalculator\Native`
* Parent class: [`\Cassandra\StringMath\DecimalCalculator`](../DecimalCalculator.md)
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Methods

### add1

Add 1 to an unsigned base-10 decimal string.

```php
public add1(string $decimal): string
```

**Parameters:**

| Parameter  | Type       | Description |
|------------|------------|-------------|
| `$decimal` | **string** |             |

**Throws:**

- [`StringMathException`](../../Exception/StringMathException.md)

***

### addUnsignedInt8

Add an unsigned 8-bit integer (0..255) to an unsigned base-10 decimal string.

```php
public addUnsignedInt8(string $decimal, int $addend): string
```

**Parameters:**

| Parameter  | Type       | Description |
|------------|------------|-------------|
| `$decimal` | **string** |             |
| `$addend`  | **int**    |             |

**Throws:**

- [`StringMathException`](../../Exception/StringMathException.md)

***

### divideBy256

Divide an unsigned base-10 decimal string by 256.

```php
public divideBy256(string $decimal): array{quotient: string, remainder: int}
```

**Parameters:**

| Parameter  | Type       | Description |
|------------|------------|-------------|
| `$decimal` | **string** |             |

**Throws:**

- [`StringMathException`](../../Exception/StringMathException.md)

***

### multiplyBy256

Multiply an unsigned base-10 decimal string by 256.

```php
public multiplyBy256(string $decimal): string
```

**Parameters:**

| Parameter  | Type       | Description |
|------------|------------|-------------|
| `$decimal` | **string** |             |

**Throws:**

- [`StringMathException`](../../Exception/StringMathException.md)

***

### sub1

Subtract 1 from an unsigned base-10 decimal string.

```php
public sub1(string $decimal): string
```

**Parameters:**

| Parameter  | Type       | Description |
|------------|------------|-------------|
| `$decimal` | **string** |             |

**Throws:**

- [`StringMathException`](../../Exception/StringMathException.md)

***

## Inherited methods

### add1

Add 1 to an unsigned base-10 decimal string.

```php
public add1(string $decimal): string
```

* This method is **abstract**.
**Parameters:**

| Parameter  | Type       | Description |
|------------|------------|-------------|
| `$decimal` | **string** |             |

**Throws:**

- [`StringMathException`](../../Exception/StringMathException.md)

***

### addUnsignedInt8

Add an unsigned 8-bit integer (0..255) to an unsigned base-10 decimal string.

```php
public addUnsignedInt8(string $decimal, int $addend): string
```

* This method is **abstract**.
**Parameters:**

| Parameter  | Type       | Description |
|------------|------------|-------------|
| `$decimal` | **string** |             |
| `$addend`  | **int**    |             |

**Throws:**

- [`StringMathException`](../../Exception/StringMathException.md)

***

### divideBy256

Divide an unsigned base-10 decimal string by 256.

```php
public divideBy256(string $decimal): array{quotient: string, remainder: int}
```

Returns quotient and sets $remainder (0..255).

* This method is **abstract**.
**Parameters:**

| Parameter  | Type       | Description |
|------------|------------|-------------|
| `$decimal` | **string** |             |

**Throws:**

- [`StringMathException`](../../Exception/StringMathException.md)

***

### fromBinary

```php
public fromBinary(string $binary): string
```

**Parameters:**

| Parameter | Type       | Description |
|-----------|------------|-------------|
| `$binary` | **string** |             |

**Throws:**

- [`StringMathException`](../../Exception/StringMathException.md)

***

### get

```php
public static get(): self
```

* This method is **static**.
**Throws:**

- [`StringMathException`](../../Exception/StringMathException.md)

***

### multiplyBy256

Multiply an unsigned base-10 decimal string by 256.

```php
public multiplyBy256(string $decimal): string
```

* This method is **abstract**.
**Parameters:**

| Parameter  | Type       | Description |
|------------|------------|-------------|
| `$decimal` | **string** |             |

**Throws:**

- [`StringMathException`](../../Exception/StringMathException.md)

***

### set

```php
public static set(self $calculator): void
```

* This method is **static**.
**Parameters:**

| Parameter     | Type     | Description |
|---------------|----------|-------------|
| `$calculator` | **self** |             |

***

### sub1

Subtract 1 from an unsigned base-10 decimal string.

```php
public sub1(string $decimal): string
```

* This method is **abstract**.
**Parameters:**

| Parameter  | Type       | Description |
|------------|------------|-------------|
| `$decimal` | **string** |             |

**Throws:**

- [`StringMathException`](../../Exception/StringMathException.md)

***

### toBinary

```php
public toBinary(string $decimal): string
```

**Parameters:**

| Parameter  | Type       | Description |
|------------|------------|-------------|
| `$decimal` | **string** |             |

**Throws:**

- [`StringMathException`](../../Exception/StringMathException.md)

***
