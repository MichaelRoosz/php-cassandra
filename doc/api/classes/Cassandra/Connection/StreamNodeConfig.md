# StreamNodeConfig

***

* Full name: `\Cassandra\Connection\StreamNodeConfig`
* Parent class: [`\Cassandra\Connection\NodeConfig`](./NodeConfig.md)
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Properties

### connectTimeoutInSeconds

```php
public float $connectTimeoutInSeconds
```

***

### timeoutInSeconds

Receive/send timeout of the transport, in seconds. Fractional values
are honoured; a value of 0 (or less) disables the timeout.

```php
public float $timeoutInSeconds
```

This is a stall timeout: it bounds how long the stream makes no
progress at all, not how long a whole request body takes, so a large
frame on a slow link does not trip it.

The default is deliberately above Cassandra's own coordinator
timeouts (range_request_timeout and request_timeout default to 10s),
so that the server gets a chance to answer with a proper error
instead of the client tearing the connection down first. Operations
with a higher server-side timeout — TRUNCATE defaults to 60s — need
a larger value.

Disabling it is not recommended: a deadline is only noticed once the
read the client is blocked in returns, so this is also how often
request timeouts and heartbeats get to be checked. At 0 a silent
server leaves the client blocked in a read forever, and neither


- **See:** \Cassandra\Connection\ConnectionOptions::$requestTimeoutInSeconds nor the heartbeat
can fire. Lower it instead if you want tighter deadlines — with the
default of 15s, a request timeout of 30s fires somewhere between 30s
and 45s.

***

### persistent

```php
public bool $persistent
```

***

### sslOptions

```php
public array<string,mixed> $sslOptions
```

***

## Methods

### __construct

```php
public __construct(string $host = 'localhost', int $port = 9042, string $username = '', string $password = '', float $connectTimeoutInSeconds = 5, float $timeoutInSeconds = 15, bool $persistent = false, array $sslOptions = []): mixed
```

**Parameters:**

| Parameter                  | Type       | Description |
|----------------------------|------------|-------------|
| `$host`                    | **string** |             |
| `$port`                    | **int**    |             |
| `$username`                | **string** |             |
| `$password`                | **string** |             |
| `$connectTimeoutInSeconds` | **float**  |             |
| `$timeoutInSeconds`        | **float**  |             |
| `$persistent`              | **bool**   |             |
| `$sslOptions`              | **array**  |             |

***

### getNodeClass

```php
public getNodeClass(): class-string<\Cassandra\Connection\IoNode>
```

***

## Inherited methods

### __construct

```php
public __construct(string $host = 'localhost', int $port = 9042, string $username = '', string $password = ''): mixed
```

**Parameters:**

| Parameter   | Type       | Description |
|-------------|------------|-------------|
| `$host`     | **string** |             |
| `$port`     | **int**    |             |
| `$username` | **string** |             |
| `$password` | **string** |             |

***

### getNodeClass

```php
public getNodeClass(): class-string<\Cassandra\Connection\IoNode>
```

* This method is **abstract**.
***
