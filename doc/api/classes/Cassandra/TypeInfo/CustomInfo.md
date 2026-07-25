# CustomInfo

***

* Full name: `\Cassandra\TypeInfo\CustomInfo`
* Parent class: [`\Cassandra\TypeInfo\TypeInfo`](./TypeInfo.md)
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Properties

### javaClassName

```php
public string $javaClassName
```

***

## Methods

### __construct

```php
public __construct(string $javaClassName): mixed
```

**Parameters:**

| Parameter        | Type       | Description |
|------------------|------------|-------------|
| `$javaClassName` | **string** |             |

***

### fromTypeDefinition

```php
public static fromTypeDefinition(array{type: \Cassandra\Type::CUSTOM, javaClassName: string} $typeDefinition): self
```

* This method is **static**.
**Parameters:**

| Parameter         | Type                                                            | Description |
|-------------------|-----------------------------------------------------------------|-------------|
| `$typeDefinition` | **array{type: \Cassandra\Type::CUSTOM, javaClassName: string}** |             |

**Throws:**

- [`TypeInfoException`](../Exception/TypeInfoException.md)

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
