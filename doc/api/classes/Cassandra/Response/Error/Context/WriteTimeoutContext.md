# WriteTimeoutContext

***

* Full name: `\Cassandra\Response\Error\Context\WriteTimeoutContext`
* Parent class: [`\Cassandra\Response\Error\Context\ErrorContext`](./ErrorContext.md)
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Properties

### consistency

```php
public \Cassandra\Consistency $consistency
```

***

### nodesAcknowledged

```php
public int $nodesAcknowledged
```

***

### nodesRequired

```php
public int $nodesRequired
```

***

### writeType

```php
public \Cassandra\Response\Error\Context\WriteType $writeType
```

***

### contentions

```php
public ?int $contentions
```

***

## Methods

### __construct

```php
public __construct(\Cassandra\Consistency $consistency, int $nodesAcknowledged, int $nodesRequired, \Cassandra\Response\Error\Context\WriteType $writeType, ?int $contentions): mixed
```

**Parameters:**

| Parameter            | Type                                            | Description |
|----------------------|-------------------------------------------------|-------------|
| `$consistency`       | **\Cassandra\Consistency**                      |             |
| `$nodesAcknowledged` | **int**                                         |             |
| `$nodesRequired`     | **int**                                         |             |
| `$writeType`         | **\Cassandra\Response\Error\Context\WriteType** |             |
| `$contentions`       | **?int**                                        |             |

***

### toArray

```php
public toArray(): array{consistency: int, nodes_acknowledged: int, nodes_required: int, write_type: string, contentions: int|null}
```

***

## Inherited methods

### __construct

```php
public __construct(): mixed
```

***

### toArray

```php
public toArray(): array<string,mixed>
```

***
