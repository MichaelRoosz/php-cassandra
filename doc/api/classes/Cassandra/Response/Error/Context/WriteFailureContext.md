# WriteFailureContext

***

* Full name: `\Cassandra\Response\Error\Context\WriteFailureContext`
* Parent class: [`\Cassandra\Response\Error\Context\ErrorContext`](./ErrorContext.md)
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Properties

### consistency

```php
public \Cassandra\Consistency $consistency
```

***

### nodesAnswered

```php
public int $nodesAnswered
```

***

### nodesRequired

```php
public int $nodesRequired
```

***

### reasonMap

```php
public ?array $reasonMap
```

***

### numFailures

```php
public ?int $numFailures
```

***

### writeType

```php
public \Cassandra\Response\Error\Context\WriteType $writeType
```

***

## Methods

### __construct

```php
public __construct(\Cassandra\Consistency $consistency, int $nodesAnswered, int $nodesRequired, array<string,int>|null $reasonMap, ?int $numFailures, \Cassandra\Response\Error\Context\WriteType $writeType): mixed
```

**Parameters:**

| Parameter        | Type                                            | Description |
|------------------|-------------------------------------------------|-------------|
| `$consistency`   | **\Cassandra\Consistency**                      |             |
| `$nodesAnswered` | **int**                                         |             |
| `$nodesRequired` | **int**                                         |             |
| `$reasonMap`     | **array<string,int>\|null**                     |             |
| `$numFailures`   | **?int**                                        |             |
| `$writeType`     | **\Cassandra\Response\Error\Context\WriteType** |             |

***

### toArray

```php
public toArray(): array{consistency: int, nodes_answered: int, nodes_required: int, write_type: string, reasonmap: array<string,int>|null, num_failures: int|null}
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
