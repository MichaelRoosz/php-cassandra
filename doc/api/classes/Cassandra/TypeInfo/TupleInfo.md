# TupleInfo

***

* Full name: `\Cassandra\TypeInfo\TupleInfo`
* Parent class: [`\Cassandra\TypeInfo\TypeInfo`](./TypeInfo.md)
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Properties

### valueTypes

```php
public array $valueTypes
```

***

## Methods

### __construct

```php
public __construct(list<\Cassandra\TypeInfo\TypeInfo> $valueTypes): mixed
```

**Parameters:**

| Parameter     | Type                                   | Description |
|---------------|----------------------------------------|-------------|
| `$valueTypes` | **list<\Cassandra\TypeInfo\TypeInfo>** |             |

***

### fromTypeDefinition

```php
public static fromTypeDefinition(array{type: \Cassandra\Type::TUPLE, valueTypes: list<\Cassandra\Type|(array{type: \Cassandra\Type}&array)>} $typeDefinition): self
```

* This method is **static**.
**Parameters:**

| Parameter         | Type                                                                                                             | Description |
|-------------------|------------------------------------------------------------------------------------------------------------------|-------------|
| `$typeDefinition` | **array{type: \Cassandra\Type::TUPLE, valueTypes: list<\Cassandra\Type\|(array{type: \Cassandra\Type}&array)>}** |             |

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
