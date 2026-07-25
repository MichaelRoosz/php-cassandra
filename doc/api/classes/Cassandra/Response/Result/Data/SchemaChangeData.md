# SchemaChangeData

***

* Full name: `\Cassandra\Response\Result\Data\SchemaChangeData`
* Parent class: [`\Cassandra\Response\Result\Data\ResultData`](./ResultData.md)
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Properties

### changeType

```php
public \Cassandra\Response\Event\Data\SchemaChangeType $changeType
```

***

### target

```php
public \Cassandra\Response\Event\Data\SchemaChangeTarget $target
```

***

### keyspace

```php
public string $keyspace
```

***

### name

```php
public ?string $name
```

***

### argumentTypes

```php
public ?string[] $argumentTypes
```

***

## Methods

### __construct

```php
public __construct(\Cassandra\Response\Event\Data\SchemaChangeType $changeType, \Cassandra\Response\Event\Data\SchemaChangeTarget $target, string $keyspace, ?string $name = null, ?array $argumentTypes = null): mixed
```

**Parameters:**

| Parameter        | Type                                                  | Description |
|------------------|-------------------------------------------------------|-------------|
| `$changeType`    | **\Cassandra\Response\Event\Data\SchemaChangeType**   |             |
| `$target`        | **\Cassandra\Response\Event\Data\SchemaChangeTarget** |             |
| `$keyspace`      | **string**                                            |             |
| `$name`          | **?string**                                           |             |
| `$argumentTypes` | **?array**                                            |             |

***

## Inherited methods

### __construct

```php
public __construct(): mixed
```

***
