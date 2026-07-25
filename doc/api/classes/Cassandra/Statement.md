# Statement

***

* Full name: `\Cassandra\Statement`
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Methods

### __construct

```php
public __construct(\Cassandra\Connection $connection, int $streamId, \Cassandra\Request\Request $request, ?\Cassandra\Request\Request $originalRequest = null, ?float $requestTimeoutInSeconds = null): mixed
```

**Parameters:**

| Parameter                  | Type                            | Description                                                                                                                                                   |
|----------------------------|---------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `$connection`              | **\Cassandra\Connection**       |                                                                                                                                                               |
| `$streamId`                | **int**                         |                                                                                                                                                               |
| `$request`                 | **\Cassandra\Request\Request**  |                                                                                                                                                               |
| `$originalRequest`         | **?\Cassandra\Request\Request** |                                                                                                                                                               |
| `$requestTimeoutInSeconds` | **?float**                      | an explicit override from the
caller, which wins over what the request's own options asked for. Null
falls back to those, and then to the connection default. |

***

### getOriginalRequest

```php
public getOriginalRequest(): \Cassandra\Request\Request
```

***

### getPreparedResult

```php
public getPreparedResult(): \Cassandra\Response\Result\PreparedResult
```

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)
- [`StatementException`](./Exception/StatementException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)

***

### getRequest

```php
public getRequest(): \Cassandra\Request\Request
```

***

### getRequestTimeout

The timeout this statement's request asked for, or null to use the
connection default.

```php
public getRequestTimeout(): ?float
```

***

### getResponse

```php
public getResponse(): \Cassandra\Response\Response
```

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)
- [`StatementException`](./Exception/StatementException.md)

***

### getResult

```php
public getResult(): \Cassandra\Response\Result
```

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)
- [`StatementException`](./Exception/StatementException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)

***

### getRowsResult

```php
public getRowsResult(): \Cassandra\Response\Result\RowsResult
```

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)
- [`StatementException`](./Exception/StatementException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)

***

### getSchemaChangeResult

```php
public getSchemaChangeResult(): \Cassandra\Response\Result\SchemaChangeResult
```

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)
- [`StatementException`](./Exception/StatementException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)

***

### getSentAt

When the request was written to the node, as a microtime.

```php
public getSentAt(): float
```

***

### getSetKeyspaceResult

```php
public getSetKeyspaceResult(): \Cassandra\Response\Result\SetKeyspaceResult
```

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)
- [`StatementException`](./Exception/StatementException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)

***

### getStreamId

```php
public getStreamId(): int
```

***

### isAbandoned

Whether this statement was given up on before it was answered — the
connection was closed, or a follow-up request it needed never reached the
node — in which case it can never be resolved and any attempt to read its
result fails immediately instead of waiting for an answer that cannot
arrive.

```php
public isAbandoned(): bool
```

***

### isAutoPreparing

```php
public isAutoPreparing(): bool
```

***

### isRepreparing

```php
public isRepreparing(): bool
```

***

### isResultReady

```php
public isResultReady(): bool
```

***

### isTimedOut

Whether the client stopped waiting for this statement's answer. The
connection and its other statements are unaffected; this one is simply
finished and would have to be sent again.

```php
public isTimedOut(): bool
```

***

### isWaitingForResult

```php
public isWaitingForResult(): bool
```

***

### setRequest

```php
public setRequest(\Cassandra\Request\Request $request): void
```

**Parameters:**

| Parameter  | Type                           | Description |
|------------|--------------------------------|-------------|
| `$request` | **\Cassandra\Request\Request** |             |

***

### setResponse

```php
public setResponse(?\Cassandra\Response\Response $response): void
```

**Parameters:**

| Parameter   | Type                              | Description |
|-------------|-----------------------------------|-------------|
| `$response` | **?\Cassandra\Response\Response** |             |

***

### setSentAt

Restarts the request timeout budget, for when the request behind this
statement is (re)written to the node.

```php
public setSentAt(float $sentAt): void
```

**Parameters:**

| Parameter | Type      | Description |
|-----------|-----------|-------------|
| `$sentAt` | **float** |             |

***

### setStatus

```php
public setStatus(\Cassandra\StatementStatus $status): void
```

**Parameters:**

| Parameter | Type                           | Description |
|-----------|--------------------------------|-------------|
| `$status` | **\Cassandra\StatementStatus** |             |

***

### tryGetPreparedResult

```php
public tryGetPreparedResult(): ?\Cassandra\Response\Result\PreparedResult
```

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)
- [`StatementException`](./Exception/StatementException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)

***

### tryGetResponse

Non-blocking: try to fetch the response if available; returns null if not ready.

```php
public tryGetResponse(): ?\Cassandra\Response\Response
```

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)
- [`StatementException`](./Exception/StatementException.md)

***

### tryGetResult

```php
public tryGetResult(): ?\Cassandra\Response\Result
```

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)
- [`StatementException`](./Exception/StatementException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)

***

### tryGetRowsResult

```php
public tryGetRowsResult(): ?\Cassandra\Response\Result\RowsResult
```

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)
- [`StatementException`](./Exception/StatementException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)

***

### tryGetSchemaChangeResult

```php
public tryGetSchemaChangeResult(): ?\Cassandra\Response\Result\SchemaChangeResult
```

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)
- [`StatementException`](./Exception/StatementException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)

***

### tryGetSetKeyspaceResult

```php
public tryGetSetKeyspaceResult(): ?\Cassandra\Response\Result\SetKeyspaceResult
```

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)
- [`StatementException`](./Exception/StatementException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)

***

### waitForResponse

```php
public waitForResponse(): void
```

**Throws:**

- [`CompressionException`](./Exception/CompressionException.md)
- [`NodeException`](./Exception/NodeException.md)
- [`RequestException`](./Exception/RequestException.md)
- [`ConnectionException`](./Exception/ConnectionException.md)
- [`ResponseException`](./Exception/ResponseException.md)
- [`ValueException`](./Exception/ValueException.md)
- [`ValueFactoryException`](./Exception/ValueFactoryException.md)
- [`ServerException`](./Exception/ServerException.md)
- [`RequestTimeoutException`](./Exception/RequestTimeoutException.md)
- [`StatementException`](./Exception/StatementException.md)

***
