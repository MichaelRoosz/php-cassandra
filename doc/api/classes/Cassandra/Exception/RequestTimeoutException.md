# RequestTimeoutException

The server did not answer within the client-side request timeout.

This is deliberately not a 

- **See:** \Cassandra\Exception\NodeException: nothing is known to be wrong
with the node or the connection, the coordinator was simply slower than the
client was willing to wait. Only the requests that ran out are finished — the
connection stays open, keeps its prepared statements, and its other requests
carry on — and the node is not counted as failed, so one expensive query
cannot push a healthy node out of rotation.

Raise the request timeout (`ConnectionOptions::$requestTimeoutInSeconds`,
`Connection::setRequestTimeout()`, the `requestTimeoutInSeconds` option of the
request itself, or the per-call argument of `Connection::syncRequest()`) for
operations that are legitimately slow, such as TRUNCATE, which Cassandra
allows 60s for by default.

***

* Full name: `\Cassandra\Exception\RequestTimeoutException`
* Parent class: [`\Cassandra\Exception\CassandraException`](./CassandraException.md)
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Methods

### __construct

```php
public __construct(string $message, int $code, array $context = [], ?\Throwable $previous = null, \Cassandra\Statement[] $timedOutStatements = []): mixed
```

**Parameters:**

| Parameter             | Type                       | Description                                                                                                                                                                                                                                                                                         |
|-----------------------|----------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `$message`            | **string**                 |                                                                                                                                                                                                                                                                                                     |
| `$code`               | **int**                    |                                                                                                                                                                                                                                                                                                     |
| `$context`            | **array**                  |                                                                                                                                                                                                                                                                                                     |
| `$previous`           | **?\Throwable**            |                                                                                                                                                                                                                                                                                                     |
| `$timedOutStatements` | **\Cassandra\Statement[]** | the statements that ran out of
time, so that a caller waiting on several of them can tell which ones to
send again without having to match stream ids up by hand. Empty for a
synchronous request, which has no statement of its own — there the failing
request is simply the one that was called. |

***

### getTimedOutStatements

The statements that ran out of time, in the order they were given up on.

```php
public getTimedOutStatements(): \Cassandra\Statement[]
```

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
