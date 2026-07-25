# Connection

***

* Full name: `\Cassandra\Connection`
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Methods

### __construct

```php
public __construct(\Cassandra\Connection\NodeConfig[] $nodes, string $keyspace = '', \Cassandra\Connection\ConnectionOptions $options = new \Cassandra\Connection\ConnectionOptions()): mixed
```

**Parameters:**

| Parameter   | Type                                        | Description |
|-------------|---------------------------------------------|-------------|
| `$nodes`    | **\Cassandra\Connection\NodeConfig[]**      |             |
| `$keyspace` | **string**                                  |             |
| `$options`  | **\Cassandra\Connection\ConnectionOptions** |             |

***

### asyncRequest

```php
public asyncRequest(\Cassandra\Request\Request $request, ?float $requestTimeoutInSeconds = null): \Cassandra\Statement
```

**Parameters:**

| Parameter                  | Type                           | Description                                                                                                                                                                                                                                                                                                            |
|----------------------------|--------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `$request`                 | **\Cassandra\Request\Request** |                                                                                                                                                                                                                                                                                                                        |
| `$requestTimeoutInSeconds` | **?float**                     | how long the server may take to
answer, overriding the request's and the connection's request timeout for
this statement only. The counterpart of the argument
{@see \Cassandra\self::syncRequest()} takes; the budget runs from now, when the
request is written, not from whenever the caller starts waiting for it. |

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)

***

### batch

```php
public batch(\Cassandra\Request\Batch $batchRequest): \Cassandra\Response\Result
```

**Parameters:**

| Parameter       | Type                         | Description |
|-----------------|------------------------------|-------------|
| `$batchRequest` | **\Cassandra\Request\Batch** |             |

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)

***

### batchAsync

```php
public batchAsync(\Cassandra\Request\Batch $batchRequest): \Cassandra\Statement
```

**Parameters:**

| Parameter       | Type                         | Description |
|-----------------|------------------------------|-------------|
| `$batchRequest` | **\Cassandra\Request\Batch** |             |

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)

***

### configureValueEncoding

```php
public configureValueEncoding(\Cassandra\Value\ValueEncodeConfig $config): void
```

**Parameters:**

| Parameter | Type                                   | Description |
|-----------|----------------------------------------|-------------|
| `$config` | **\Cassandra\Value\ValueEncodeConfig** |             |

***

### connect

```php
public connect(): void
```

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)

***

### createBatchRequest

```php
public createBatchRequest(\Cassandra\Request\BatchType $type = \Cassandra\Request\BatchType::LOGGED, ?\Cassandra\Consistency $consistency = null, \Cassandra\Request\Options\BatchOptions $options = new \Cassandra\Request\Options\BatchOptions()): \Cassandra\Request\Batch
```

**Parameters:**

| Parameter      | Type                                        | Description |
|----------------|---------------------------------------------|-------------|
| `$type`        | **\Cassandra\Request\BatchType**            |             |
| `$consistency` | **?\Cassandra\Consistency**                 |             |
| `$options`     | **\Cassandra\Request\Options\BatchOptions** |             |

***

### disconnect

```php
public disconnect(): void
```

***

### drainAvailableResponses

Non-blocking: read up to $max available responses, returning how many were processed.

```php
public drainAvailableResponses(int $max = \Cassandra\PHP_INT_MAX): int
```

NOTE: This method will not block; it processes any currently available responses
and returns when the receive buffer is drained or the provided limit is reached.

**Parameters:**

| Parameter | Type    | Description |
|-----------|---------|-------------|
| `$max`    | **int** |             |

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)

***

### execute

```php
public execute(\Cassandra\Response\Result $previousResult, array $values = [], ?\Cassandra\Consistency $consistency = null, \Cassandra\Request\Options\ExecuteOptions $options = new \Cassandra\Request\Options\ExecuteOptions()): \Cassandra\Response\Result
```

**Parameters:**

| Parameter         | Type                                          | Description |
|-------------------|-----------------------------------------------|-------------|
| `$previousResult` | **\Cassandra\Response\Result**                |             |
| `$values`         | **array**                                     |             |
| `$consistency`    | **?\Cassandra\Consistency**                   |             |
| `$options`        | **\Cassandra\Request\Options\ExecuteOptions** |             |

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)

***

### executeAll

```php
public executeAll(\Cassandra\Response\Result $previousResult, array $values = [], ?\Cassandra\Consistency $consistency = null, \Cassandra\Request\Options\ExecuteOptions $options = new \Cassandra\Request\Options\ExecuteOptions()): \Cassandra\Response\Result\RowsResult[]
```

**Parameters:**

| Parameter         | Type                                          | Description |
|-------------------|-----------------------------------------------|-------------|
| `$previousResult` | **\Cassandra\Response\Result**                |             |
| `$values`         | **array**                                     |             |
| `$consistency`    | **?\Cassandra\Consistency**                   |             |
| `$options`        | **\Cassandra\Request\Options\ExecuteOptions** |             |

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)

***

### executeAsync

```php
public executeAsync(\Cassandra\Response\Result $previousResult, array $values = [], ?\Cassandra\Consistency $consistency = null, \Cassandra\Request\Options\ExecuteOptions $options = new \Cassandra\Request\Options\ExecuteOptions()): \Cassandra\Statement
```

**Parameters:**

| Parameter         | Type                                          | Description |
|-------------------|-----------------------------------------------|-------------|
| `$previousResult` | **\Cassandra\Response\Result**                |             |
| `$values`         | **array**                                     |             |
| `$consistency`    | **?\Cassandra\Consistency**                   |             |
| `$options`        | **\Cassandra\Request\Options\ExecuteOptions** |             |

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)

***

### getNode

```php
public getNode(): ?\Cassandra\Connection\Node
```

***

### getProtocolVersion

Returns the protocol version used by this connection.

```php
public getProtocolVersion(): \Cassandra\Protocol\ProtocolVersion
```

Before connecting, it will return the initial protocol version,
as set in the connection options.

***

### getResponseForStatement

Wait for this statement's answer and return it.

```php
public getResponseForStatement(\Cassandra\Statement $statement): \Cassandra\Response\Response
```

**Parameters:**

| Parameter    | Type                     | Description |
|--------------|--------------------------|-------------|
| `$statement` | **\Cassandra\Statement** |             |

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)
- [`StatementException`](./Exception/StatementException.md)

***

### getVersion

```php
public getVersion(): int
```

* **Warning:** this method is **deprecated**. This means that this method will likely be removed in a future version.
***

### isConnected

```php
public isConnected(): bool
```

***

### prepare

```php
public prepare(string $query, \Cassandra\Request\Options\PrepareOptions $options = new \Cassandra\Request\Options\PrepareOptions()): \Cassandra\Response\Result\PreparedResult
```

**Parameters:**

| Parameter  | Type                                          | Description |
|------------|-----------------------------------------------|-------------|
| `$query`   | **string**                                    |             |
| `$options` | **\Cassandra\Request\Options\PrepareOptions** |             |

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)

***

### prepareAsync

```php
public prepareAsync(string $query, \Cassandra\Request\Options\PrepareOptions $options = new \Cassandra\Request\Options\PrepareOptions()): \Cassandra\Statement
```

**Parameters:**

| Parameter  | Type                                          | Description |
|------------|-----------------------------------------------|-------------|
| `$query`   | **string**                                    |             |
| `$options` | **\Cassandra\Request\Options\PrepareOptions** |             |

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)

***

### query

```php
public query(string $query, array $values = [], ?\Cassandra\Consistency $consistency = null, \Cassandra\Request\Options\QueryOptions $options = new \Cassandra\Request\Options\QueryOptions()): \Cassandra\Response\Result
```

**Parameters:**

| Parameter      | Type                                        | Description |
|----------------|---------------------------------------------|-------------|
| `$query`       | **string**                                  |             |
| `$values`      | **array**                                   |             |
| `$consistency` | **?\Cassandra\Consistency**                 |             |
| `$options`     | **\Cassandra\Request\Options\QueryOptions** |             |

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)

***

### queryAll

```php
public queryAll(string $query, array $values = [], ?\Cassandra\Consistency $consistency = null, \Cassandra\Request\Options\QueryOptions $options = new \Cassandra\Request\Options\QueryOptions()): \Cassandra\Response\Result\RowsResult[]
```

**Parameters:**

| Parameter      | Type                                        | Description |
|----------------|---------------------------------------------|-------------|
| `$query`       | **string**                                  |             |
| `$values`      | **array**                                   |             |
| `$consistency` | **?\Cassandra\Consistency**                 |             |
| `$options`     | **\Cassandra\Request\Options\QueryOptions** |             |

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)

***

### queryAsync

```php
public queryAsync(string $query, array $values = [], ?\Cassandra\Consistency $consistency = null, \Cassandra\Request\Options\QueryOptions $options = new \Cassandra\Request\Options\QueryOptions()): \Cassandra\Statement
```

**Parameters:**

| Parameter      | Type                                        | Description |
|----------------|---------------------------------------------|-------------|
| `$query`       | **string**                                  |             |
| `$values`      | **array**                                   |             |
| `$consistency` | **?\Cassandra\Consistency**                 |             |
| `$options`     | **\Cassandra\Request\Options\QueryOptions** |             |

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)

***

### registerEventListener

```php
public registerEventListener(\Cassandra\EventListener $eventListener): void
```

**Parameters:**

| Parameter        | Type                         | Description |
|------------------|------------------------------|-------------|
| `$eventListener` | **\Cassandra\EventListener** |             |

***

### registerWarningsListener

```php
public registerWarningsListener(\Cassandra\WarningsListener $warningsListener): void
```

**Parameters:**

| Parameter           | Type                            | Description |
|---------------------|---------------------------------|-------------|
| `$warningsListener` | **\Cassandra\WarningsListener** |             |

***

### setConsistency

```php
public setConsistency(\Cassandra\Consistency $consistency): void
```

**Parameters:**

| Parameter      | Type                       | Description |
|----------------|----------------------------|-------------|
| `$consistency` | **\Cassandra\Consistency** |             |

***

### setKeyspace

```php
public setKeyspace(string $keyspace): void
```

**Parameters:**

| Parameter   | Type       | Description |
|-------------|------------|-------------|
| `$keyspace` | **string** |             |

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)

***

### setRequestTimeout

How long to wait for the server's answer to a request before giving up
with a {@see \Cassandra\Exception\RequestTimeoutException}, in seconds.

```php
public setRequestTimeout(?float $requestTimeoutInSeconds): void
```

Null waits indefinitely.

Applies to every subsequent blocking call that has no explicit timeout of
its own. Raise it around operations Cassandra allows more time for, such
as TRUNCATE (60s server-side by default), or pass the timeout directly to


- **See:** \Cassandra\self::syncRequest() for a single request.

**Parameters:**

| Parameter                  | Type       | Description |
|----------------------------|------------|-------------|
| `$requestTimeoutInSeconds` | **?float** |             |

***

### supportsKeyspaceRequestOption

```php
public supportsKeyspaceRequestOption(): bool
```

***

### supportsNowInSecondsRequestOption

```php
public supportsNowInSecondsRequestOption(): bool
```

***

### syncRequest

```php
public syncRequest(\Cassandra\Request\Request $request, ?float $requestTimeoutInSeconds = null): \Cassandra\Response\Response
```

**Parameters:**

| Parameter                  | Type                           | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|----------------------------|--------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `$request`                 | **\Cassandra\Request\Request** |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `$requestTimeoutInSeconds` | **?float**                     | how long to wait for the server's
answer, overriding the request's and the connection's request timeout for this call only.
Pass a larger value for operations Cassandra itself allows more time for,
such as TRUNCATE.

It bounds each request this call sends, not the call as a whole: when the
driver has to prepare or reprepare the statement first, the PREPARE and
the request it precedes each get the full budget, so the call can take a
multiple of it before giving up. |

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)

***

### tryReadNextEvent

Non-blocking: attempt to read and return the next event, or null if none is available.

```php
public tryReadNextEvent(): ?\Cassandra\Response\Event
```

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)

***

### tryReadNextResponse

Non-blocking: attempt to read and return the next response, or null if none is available.

```php
public tryReadNextResponse(): ?\Cassandra\Response\Response
```

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)

***

### tryResolveStatement

Non-blocking: try to resolve a specific statement; returns true if it is ready.

```php
public tryResolveStatement(\Cassandra\Statement $statement): bool
```

**Parameters:**

| Parameter    | Type                     | Description |
|--------------|--------------------------|-------------|
| `$statement` | **\Cassandra\Statement** |             |

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)
- [`StatementException`](./Exception/StatementException.md)

***

### tryResolveStatements

Non-blocking: try to resolve from a set of statements, up to $max responses processed.

```php
public tryResolveStatements(\Cassandra\Statement[] $statements, int $max = \Cassandra\PHP_INT_MAX): int
```

Returns the number of newly resolved statements from the provided set.

**Parameters:**

| Parameter     | Type                       | Description |
|---------------|----------------------------|-------------|
| `$statements` | **\Cassandra\Statement[]** |             |
| `$max`        | **int**                    |             |

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)
- [`StatementException`](./Exception/StatementException.md)

***

### unregisterEventListener

```php
public unregisterEventListener(\Cassandra\EventListener $eventListener): void
```

**Parameters:**

| Parameter        | Type                         | Description |
|------------------|------------------------------|-------------|
| `$eventListener` | **\Cassandra\EventListener** |             |

***

### unregisterWarningsListener

```php
public unregisterWarningsListener(\Cassandra\WarningsListener $warningsListener): void
```

**Parameters:**

| Parameter           | Type                            | Description |
|---------------------|---------------------------------|-------------|
| `$warningsListener` | **\Cassandra\WarningsListener** |             |

***

### waitForAllPendingStatements

Wait until every statement in flight has been answered.

```php
public waitForAllPendingStatements(?float $timeoutInSeconds = null): void
```

**Parameters:**

| Parameter           | Type       | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
|---------------------|------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `$timeoutInSeconds` | **?float** | how long this call may block:
  null  let the statements' own budgets bound it (the default)
  0     do not wait: return as soon as there is nothing more to read
  n     wait at most n seconds
  INF   wait for as long as it takes

It returns once every statement has been answered or the time is up. A
statement that runs out of its own budget is given up on and raises a
RequestTimeoutException instead.

A timeout of 0 still costs one read, but a non-blocking one, so it does
not wait on the transport either; {@see \Cassandra\self::tryResolveStatements()} is
the equivalent that never touches a deadline at all. |

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)

***

### waitForAnyStatement

Wait until any of the given statements becomes ready and return it.

```php
public waitForAnyStatement(\Cassandra\Statement[] $statements, ?float $timeoutInSeconds = null): ?\Cassandra\Statement
```

**Parameters:**

| Parameter           | Type                       | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
|---------------------|----------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `$statements`       | **\Cassandra\Statement[]** |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `$timeoutInSeconds` | **?float**                 | how long this call may block:
  null  let the statements' own budgets bound it (the default)
  0     do not wait: return as soon as there is nothing more to read
  n     wait at most n seconds
  INF   wait for as long as it takes

Returns null when the time is up with none of them ready; the statements
are untouched and can still be waited on. A statement that runs out of
its own budget is given up on and raises a RequestTimeoutException.

A timeout of 0 still costs one read, but a non-blocking one, so it does
not wait on the transport either; {@see \Cassandra\self::tryResolveStatements()} is
the equivalent that never touches a deadline at all. |

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)
- [`StatementException`](./Exception/StatementException.md)

***

### waitForNextEvent

Wait for the next server event.

```php
public waitForNextEvent(?float $timeoutInSeconds = null): ?\Cassandra\Response\Event
```

An idle event stream is not an error, so this keeps waiting across
transport read timeouts instead of tearing the connection down: the node
simply had nothing to report. While waiting, an OPTIONS heartbeat is sent
whenever the connection has been silent for longer than the configured
heartbeat interval, so a connection that died quietly is still noticed —
which is the job a read timeout cannot do here.

**Parameters:**

| Parameter           | Type       | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
|---------------------|------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `$timeoutInSeconds` | **?float** | how long this call may block:
  null  wait for as long as it takes (the default), since an event can
        arrive at any time
  0     do not wait: return as soon as there is nothing more to read
  n     wait at most n seconds
  INF   wait for as long as it takes

Returns null when the time is up without an event, leaving the connection
usable. A timeout of 0 still costs one read, but a non-blocking one, so
it does not wait on the transport either; {@see \Cassandra\self::tryReadNextEvent()}
is the equivalent that never touches a deadline at all.

Requests already in flight keep their own deadlines while this waits, and
one that runs out is given up on here. It is not raised here, though: an
event listener did not ask about it, so its loop is not interrupted for
it — the caller finds out from the statement, which then raises
RequestTimeoutException. Only that request is affected; the connection
stays open. |

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)

***

### waitForNextResponse

Wait for the next response of any kind, the counterpart of
{@see self::waitForNextEvent()}.

```php
public waitForNextResponse(?float $timeoutInSeconds = null): ?\Cassandra\Response\Response
```

**Parameters:**

| Parameter           | Type       | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|---------------------|------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `$timeoutInSeconds` | **?float** | how long this call may block:
  null  use the connection's request timeout (the default)
  0     do not wait: return as soon as there is nothing more to read
  n     wait at most n seconds
  INF   wait for as long as it takes

Null means the connection's request timeout here, rather than "no bound"
as it does in the waits that take statements: those are bounded by the
budgets of the statements they were given, and this call has none to go
by. Pass INF for a wait that only ends when something arrives.

Returns null when the time is up with nothing having arrived and nothing
overdue. A timeout of 0 still costs one read, but a non-blocking one, so
it does not wait on the transport either;
{@see \Cassandra\self::tryReadNextResponse()} is the equivalent that never touches a
deadline at all.

Requests already in flight keep their own deadlines while this waits, and
one that runs out is given up on here. It is not raised here, though: the
caller asked for the next response, not about any request in particular —
they find out from the statement, which then raises
RequestTimeoutException. Only that request is affected; the connection
stays open. |

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)

***

### waitForStatements

Wait until the given async statements have been answered.

```php
public waitForStatements(\Cassandra\Statement[] $statements, ?float $timeoutInSeconds = null): void
```

**Parameters:**

| Parameter           | Type                       | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
|---------------------|----------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `$statements`       | **\Cassandra\Statement[]** |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `$timeoutInSeconds` | **?float**                 | how long this call may block:
  null  let the statements' own budgets bound it (the default)
  0     do not wait: return as soon as there is nothing more to read
  n     wait at most n seconds
  INF   wait for as long as it takes

It returns once they have all been answered or the time is up, so check
isResultReady() when passing a timeout. A statement that runs out of its
own budget is given up on and raises a RequestTimeoutException instead.

A timeout of 0 still costs one read, but a non-blocking one, so it does
not wait on the transport either; {@see \Cassandra\self::tryResolveStatements()} is
the equivalent that never touches a deadline at all. |

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)
- [`StatementException`](./Exception/StatementException.md)

***

### withConsistency

```php
public withConsistency(\Cassandra\Consistency $consistency): self
```

**Parameters:**

| Parameter      | Type                       | Description |
|----------------|----------------------------|-------------|
| `$consistency` | **\Cassandra\Consistency** |             |

***

### withKeyspace

```php
public withKeyspace(string $keyspace): self
```

**Parameters:**

| Parameter   | Type       | Description |
|-------------|------------|-------------|
| `$keyspace` | **string** |             |

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)

***
