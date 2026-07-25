# ReadTimeoutContext

***

* Full name: `\Cassandra\Response\Error\Context\ReadTimeoutContext`
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

### dataPresent

```php
public bool $dataPresent
```

***

## Methods

### __construct

```php
public __construct(\Cassandra\Consistency $consistency, int $nodesAnswered, int $nodesRequired, bool $dataPresent): mixed
```

**Parameters:**

| Parameter        | Type                       | Description |
|------------------|----------------------------|-------------|
| `$consistency`   | **\Cassandra\Consistency** |             |
| `$nodesAnswered` | **int**                    |             |
| `$nodesRequired` | **int**                    |             |
| `$dataPresent`   | **bool**                   |             |

***

### toArray

```php
public toArray(): array{consistency: int, nodes_answered: int, nodes_required: int, data_present: bool}
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
