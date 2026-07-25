# ValueFactory

***

* Full name: `\Cassandra\ValueFactory`
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Methods

### getBinaryByTypeInfo

```php
public static getBinaryByTypeInfo(\Cassandra\TypeInfo\TypeInfo $typeInfo, mixed $value): string
```

* This method is **static**.
**Parameters:**

| Parameter   | Type                             | Description |
|-------------|----------------------------------|-------------|
| `$typeInfo` | **\Cassandra\TypeInfo\TypeInfo** |             |
| `$value`    | **mixed**                        |             |

**Throws:**

- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ValueException`](./Exception/ValueException.md)

***

### getSerializedLengthOfType

```php
public static getSerializedLengthOfType(\Cassandra\Type $type): int
```

* This method is **static**.
**Parameters:**

| Parameter | Type                | Description |
|-----------|---------------------|-------------|
| `$type`   | **\Cassandra\Type** |             |

**Throws:**

- [`ValueFactoryException`](./Exception/ValueFactoryException.md)

***

### getTypeInfoFromType

```php
public static getTypeInfoFromType(\Cassandra\Type $type): \Cassandra\TypeInfo\TypeInfo
```

* This method is **static**.
**Parameters:**

| Parameter | Type                | Description |
|-----------|---------------------|-------------|
| `$type`   | **\Cassandra\Type** |             |

**Throws:**

- [`ValueFactoryException`](./Exception/ValueFactoryException.md)

***

### getTypeInfoFromTypeDefinition

```php
public static getTypeInfoFromTypeDefinition(array|\Cassandra\Type $typeDefinition): \Cassandra\TypeInfo\TypeInfo
```

* This method is **static**.
**Parameters:**

| Parameter         | Type                       | Description |
|-------------------|----------------------------|-------------|
| `$typeDefinition` | **array\|\Cassandra\Type** |             |

**Throws:**

- [`TypeInfoException`](./Exception/TypeInfoException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ValueException`](./Exception/ValueException.md)

***

### getValueObjectFromBinary

```php
public static getValueObjectFromBinary(\Cassandra\TypeInfo\TypeInfo $typeInfo, string $binary, ?\Cassandra\Value\ValueEncodeConfig $valueEncodeConfig = null): \Cassandra\Value\ValueBase
```

* This method is **static**.
**Parameters:**

| Parameter            | Type                                    | Description |
|----------------------|-----------------------------------------|-------------|
| `$typeInfo`          | **\Cassandra\TypeInfo\TypeInfo**        |             |
| `$binary`            | **string**                              |             |
| `$valueEncodeConfig` | **?\Cassandra\Value\ValueEncodeConfig** |             |

**Throws:**

- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ValueException`](./Exception/ValueException.md)

***

### getValueObjectFromStream

```php
public static getValueObjectFromStream(\Cassandra\TypeInfo\TypeInfo $typeInfo, ?int $length, \Cassandra\Response\StreamReader $stream, ?\Cassandra\Value\ValueEncodeConfig $valueEncodeConfig = null): \Cassandra\Value\ValueBase
```

* This method is **static**.
**Parameters:**

| Parameter            | Type                                    | Description |
|----------------------|-----------------------------------------|-------------|
| `$typeInfo`          | **\Cassandra\TypeInfo\TypeInfo**        |             |
| `$length`            | **?int**                                |             |
| `$stream`            | **\Cassandra\Response\StreamReader**    |             |
| `$valueEncodeConfig` | **?\Cassandra\Value\ValueEncodeConfig** |             |

**Throws:**

- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ValueException`](./Exception/ValueException.md)

***

### getValueObjectFromValue

```php
public static getValueObjectFromValue(\Cassandra\TypeInfo\TypeInfo $typeInfo, mixed $value): ?\Cassandra\Value\ValueBase
```

* This method is **static**.
**Parameters:**

| Parameter   | Type                             | Description |
|-------------|----------------------------------|-------------|
| `$typeInfo` | **\Cassandra\TypeInfo\TypeInfo** |             |
| `$value`    | **mixed**                        |             |

**Throws:**

- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ValueException`](./Exception/ValueException.md)

***

### isSerializedAsFixedLength

```php
public static isSerializedAsFixedLength(\Cassandra\Type $type): bool
```

* This method is **static**.
**Parameters:**

| Parameter | Type                | Description |
|-----------|---------------------|-------------|
| `$type`   | **\Cassandra\Type** |             |

**Throws:**

- [`ValueFactoryException`](./Exception/ValueFactoryException.md)

***

### isSimpleType

```php
public static isSimpleType(\Cassandra\Type $type): bool
```

* This method is **static**.
**Parameters:**

| Parameter | Type                | Description |
|-----------|---------------------|-------------|
| `$type`   | **\Cassandra\Type** |             |

**Throws:**

- [`ValueFactoryException`](./Exception/ValueFactoryException.md)

***
