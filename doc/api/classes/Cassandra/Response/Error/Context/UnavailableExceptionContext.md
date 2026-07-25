# UnavailableExceptionContext

***

* Full name: `\Cassandra\Response\Error\Context\UnavailableExceptionContext`
* Parent class: [`\Cassandra\Response\Error\Context\ErrorContext`](./ErrorContext.md)
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Properties

### consistency

```php
public \Cassandra\Consistency $consistency
```

***

### nodesRequired

```php
public int $nodesRequired
```

***

### nodesAlive

```php
public int $nodesAlive
```

***

## Methods

### __construct

```php
public __construct(\Cassandra\Consistency $consistency, int $nodesRequired, int $nodesAlive): mixed
```

**Parameters:**

| Parameter        | Type                       | Description |
|------------------|----------------------------|-------------|
| `$consistency`   | **\Cassandra\Consistency** |             |
| `$nodesRequired` | **int**                    |             |
| `$nodesAlive`    | **int**                    |             |

***

### toArray

```php
public toArray(): array{consistency: int, nodes_required: int, nodes_alive: int}
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
