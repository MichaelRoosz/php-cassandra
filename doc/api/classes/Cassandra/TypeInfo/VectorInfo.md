# VectorInfo

***

* Full name: `\Cassandra\TypeInfo\VectorInfo`
* Parent class: [`\Cassandra\TypeInfo\TypeInfo`](./TypeInfo.md)
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Properties

### valueType

```php
public \Cassandra\TypeInfo\TypeInfo $valueType
```

***

### dimensions

```php
public int $dimensions
```

***

## Methods

### __construct

```php
public __construct(\Cassandra\TypeInfo\TypeInfo $valueType, int $dimensions): mixed
```

**Parameters:**

| Parameter     | Type                             | Description |
|---------------|----------------------------------|-------------|
| `$valueType`  | **\Cassandra\TypeInfo\TypeInfo** |             |
| `$dimensions` | **int**                          |             |

***

### fromTypeDefinition

```php
public static fromTypeDefinition(array{type: \Cassandra\Type::VECTOR, valueType: \Cassandra\Type|(array{type: \Cassandra\Type}&array), dimensions: int} $typeDefinition): self
```

* This method is **static**.
**Parameters:**

| Parameter         | Type                                                                                                                        | Description |
|-------------------|-----------------------------------------------------------------------------------------------------------------------------|-------------|
| `$typeDefinition` | **array{type: \Cassandra\Type::VECTOR, valueType: \Cassandra\Type\|(array{type: \Cassandra\Type}&array), dimensions: int}** |             |

**Throws:**

- [`TypeInfoException`](../Exception/TypeInfoException.md)
- [`ValueException`](../Exception/ValueException.md)
- [`ValueFactoryException`](../Exception/ValueFactoryException.md)

***

## Inherited methods

### __construct

```php
public __construct(\Cassandra\Type $type): mixed
```

**Parameters:**

| Parameter | Type                | Description |
|-----------|---------------------|-------------|
| `$type`   | **\Cassandra\Type** |             |

***
