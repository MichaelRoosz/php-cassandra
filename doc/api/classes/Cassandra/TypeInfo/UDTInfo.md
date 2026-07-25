# UDTInfo

***

* Full name: `\Cassandra\TypeInfo\UDTInfo`
* Parent class: [`\Cassandra\TypeInfo\TypeInfo`](./TypeInfo.md)
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Properties

### valueTypes

```php
public array<string,\Cassandra\TypeInfo\TypeInfo> $valueTypes
```

***

### isFrozen

```php
public bool $isFrozen
```

***

### keyspace

```php
public ?string $keyspace
```

***

### name

```php
public ?string $name
```

***

## Methods

### __construct

```php
public __construct(array $valueTypes, bool $isFrozen, ?string $keyspace = null, ?string $name = null): mixed
```

**Parameters:**

| Parameter     | Type        | Description |
|---------------|-------------|-------------|
| `$valueTypes` | **array**   |             |
| `$isFrozen`   | **bool**    |             |
| `$keyspace`   | **?string** |             |
| `$name`       | **?string** |             |

***

### fromTypeDefinition

```php
public static fromTypeDefinition(array{type: \Cassandra\Type::UDT, valueTypes: array<string,\Cassandra\Type|(array{type: \Cassandra\Type}&array)>, isFrozen: bool, keyspace?: string, name?: string} $typeDefinition): self
```

* This method is **static**.
**Parameters:**

| Parameter         | Type                                                                                                                                                                     | Description |
|-------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| `$typeDefinition` | **array{type: \Cassandra\Type::UDT, valueTypes: array<string,\Cassandra\Type\|(array{type: \Cassandra\Type}&array)>, isFrozen: bool, keyspace?: string, name?: string}** |             |

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
