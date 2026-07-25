# SocketNodeConfig

***

* Full name: `\Cassandra\Connection\SocketNodeConfig`
* Parent class: [`\Cassandra\Connection\NodeConfig`](./NodeConfig.md)
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Constants

| Constant              | Visibility | Type | Value                      |
|-----------------------|------------|------|----------------------------|
| `DEFAULT_SO_RCVTIMEO` | public     |      | ['sec' => 15, 'usec' => 0] |
| `DEFAULT_SO_SNDTIMEO` | public     |      | ['sec' => 10, 'usec' => 0] |

## Properties

### socketOptions

```php
public (int|array)[] $socketOptions
```

***

### connectTimeoutInSeconds

Timeout for establishing the connection, in seconds. Fractional
values are allowed; it must be greater than zero, as an unbounded
connect would let an unreachable host wedge the client for as long
as the kernel keeps retrying.

```php
public float $connectTimeoutInSeconds
```

***

## Methods

### __construct

```php
public __construct(string $host = 'localhost', int $port = 9042, string $username = '', string $password = '', (int|array)[] $socketOptions = [\Cassandra\Connection\SO_RCVTIMEO => \self::DEFAULT_SO_RCVTIMEO, \Cassandra\Connection\SO_SNDTIMEO => \self::DEFAULT_SO_SNDTIMEO], float $connectTimeoutInSeconds = 5): mixed
```

**Parameters:**

| Parameter                  | Type               | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
|----------------------------|--------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `$host`                    | **string**         |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `$port`                    | **int**            |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `$username`                | **string**         |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `$password`                | **string**         |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `$socketOptions`           | **(int\|array)[]** | 
see https://www.php.net/manual/en/function.socket-get-option.php

SO_RCVTIMEO / SO_SNDTIMEO drive the receive/send timeouts of the
transport. Both the 'sec' and the 'usec' component are honoured, so
sub-second timeouts work; `['sec' => 0, 'usec' => 0]` disables the
timeout, matching the meaning of the socket option itself.

Disabling SO_RCVTIMEO is not recommended: a deadline is only noticed once
the read the client is blocked in returns, so the receive timeout is also
how often request timeouts and heartbeats get to be checked. Without it a
silent server leaves the client blocked in a read forever, and neither
{@see \Cassandra\Connection\ConnectionOptions::$requestTimeoutInSeconds}
nor the heartbeat can fire. Lower it instead if you want tighter
deadlines — with the default of 15s, a request timeout of 30s fires
somewhere between 30s and 45s. |
| `$connectTimeoutInSeconds` | **float**          |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |

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
