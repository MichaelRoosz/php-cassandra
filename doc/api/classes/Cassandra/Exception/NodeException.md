# NodeException

***

* Full name: `\Cassandra\Exception\NodeException`
* Parent class: [`\Cassandra\Exception\CassandraException`](./CassandraException.md)

## Methods

### isReadTimeout

Whether this exception is a transport read timeout, i.e. the connection
produced no data within its stall window.

```php
public isReadTimeout(): bool
```

Such a timeout says nothing about the health of the connection: the
response reader keeps whatever it already consumed, so waiting longer is
always safe. Only the caller's own deadline can decide when to give up —
a slow query and an idle event stream both look exactly like this.

***

## Inherited methods

### __construct

```php
public __construct(string $message, int $code, array $context = [], ?\Throwable $previous = null): mixed
```

**Parameters:**

| Parameter   | Type            | Description |
|-------------|-----------------|-------------|
| `$message`  | **string**      |             |
| `$code`     | **int**         |             |
| `$context`  | **array**       |             |
| `$previous` | **?\Throwable** |             |

***

### context

```php
public context(): array
```

**Return Value:**

$context

***

### getContext

```php
public getContext(): array
```

**Return Value:**

$context

***
