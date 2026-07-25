# SimpleTypeInfo

***

* Full name: `\Cassandra\TypeInfo\SimpleTypeInfo`
* Parent class: [`\Cassandra\TypeInfo\TypeInfo`](./TypeInfo.md)
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Methods

### __construct

```php
public __construct(\Cassandra\Type $type): mixed
```

**Parameters:**

| Parameter | Type                | Description |
|-----------|---------------------|-------------|
| `$type`   | **\Cassandra\Type** |             |

***

### fromTypeDefinition

```php
public static fromTypeDefinition(array{type: \Cassandra\Type} $typeDefinition): self
```

* This method is **static**.
**Parameters:**

| Parameter         | Type                             | Description                                                                                         |
|-------------------|----------------------------------|-----------------------------------------------------------------------------------------------------|
| `$typeDefinition` | **array{type: \Cassandra\Type}** | 

@throws \Cassandra\Exception\TypeInfoException
@throws \Cassandra\Exception\ValueFactoryException |

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
