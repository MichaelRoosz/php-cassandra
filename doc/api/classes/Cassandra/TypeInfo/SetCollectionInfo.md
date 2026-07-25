# SetCollectionInfo

***

* Full name: `\Cassandra\TypeInfo\SetCollectionInfo`
* Parent class: [`\Cassandra\TypeInfo\TypeInfo`](./TypeInfo.md)
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Properties

### valueType

```php
public \Cassandra\TypeInfo\TypeInfo $valueType
```

***

### isFrozen

```php
public bool $isFrozen
```

***

## Methods

### __construct

```php
public __construct(\Cassandra\TypeInfo\TypeInfo $valueType, bool $isFrozen): mixed
```

**Parameters:**

| Parameter    | Type                             | Description |
|--------------|----------------------------------|-------------|
| `$valueType` | **\Cassandra\TypeInfo\TypeInfo** |             |
| `$isFrozen`  | **bool**                         |             |

***

### fromTypeDefinition

```php
public static fromTypeDefinition(array{type: \Cassandra\Type::SET, valueType: \Cassandra\Type|(array{type: \Cassandra\Type}&array), isFrozen: bool} $typeDefinition): self
```

* This method is **static**.
**Parameters:**

| Parameter         | Type                                                                                                                    | Description |
|-------------------|-------------------------------------------------------------------------------------------------------------------------|-------------|
| `$typeDefinition` | **array{type: \Cassandra\Type::SET, valueType: \Cassandra\Type\|(array{type: \Cassandra\Type}&array), isFrozen: bool}** |             |

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
