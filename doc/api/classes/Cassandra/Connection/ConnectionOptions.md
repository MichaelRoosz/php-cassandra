# ConnectionOptions

***

* Full name: `\Cassandra\Connection\ConnectionOptions`
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Properties

### enableCompression

```php
public bool $enableCompression
```

***

### throwOnOverload

```php
public bool $throwOnOverload
```

***

### nodeSelectionStrategy

```php
public \Cassandra\Connection\NodeSelectionStrategy $nodeSelectionStrategy
```

***

### preparedResultCacheSize

```php
public int $preparedResultCacheSize
```

***

### allowedProtocolVersions

```php
public \Cassandra\Protocol\ProtocolVersion[] $allowedProtocolVersions
```

***

### initialProtocolVersion

```php
public \Cassandra\Protocol\ProtocolVersion $initialProtocolVersion
```

***

### requestTimeoutInSeconds

How long to wait for the server's answer to a request, in seconds,
before giving up with a {@see \Cassandra\Exception\RequestTimeoutException}.

```php
public ?float $requestTimeoutInSeconds
```

Null waits indefinitely.

This is the client-side counterpart of Cassandra's own coordinator
timeouts, and it is what governs a slow query — not the transport
timeouts, which only bound how long a connection may be completely
silent. Operations that are legitimately slower than this default,
TRUNCATE above all (60s server-side), need a larger value; see


- **See:** \Cassandra\Connection::setRequestTimeout() and the per-call
argument of 
- **See:** \Cassandra\Connection::syncRequest().

***

### maxOrphanedStreams

How many timed-out async statements a connection may accumulate
before it is closed instead.

```php
public int $maxOrphanedStreams
```

Giving up on an async statement leaves the connection and its other
statements alone, at the cost of holding that statement's stream id
back until its late answer arrives — the id cannot be reused before
then without risking one request being resolved by another's
response. A server that keeps timing out would therefore tie up more
and more ids, so past this many the connection is dropped and starts
over with a clean set.

***

### heartbeatIntervalInSeconds

How long the connection may stay silent before an OPTIONS heartbeat
is sent to prove it is still alive, in seconds. Null disables
heartbeats.

```php
public ?float $heartbeatIntervalInSeconds
```

This is the only thing that can tell a dead connection from a quiet
one, whether the client is waiting for an event or for the answer to
a slow request — both look identical at the socket. Because stream
ids are multiplexed, the heartbeat is answered on its own stream
while a slow request is still being computed, so a broken connection
surfaces after roughly this interval plus the heartbeat timeout,
however generous the request timeout is.

Roughly, because a silent connection is only looked at between reads:
the transport's stall window is how often the interval and the
timeout below get to be judged, so both are rounded up to a multiple
of it. Lower that window if the heartbeat has to bite sooner.

***

### heartbeatTimeoutInSeconds

How long to wait for the answer to a heartbeat before treating the
connection as dead, in seconds. Like the interval above, it is judged
at the granularity of the transport's stall window, so setting it
below that window does not make detection any quicker.

```php
public float $heartbeatTimeoutInSeconds
```

***

## Methods

### __construct

```php
public __construct(bool $enableCompression = false, bool $throwOnOverload = false, \Cassandra\Connection\NodeSelectionStrategy $nodeSelectionStrategy = \Cassandra\Connection\NodeSelectionStrategy::Random, int $preparedResultCacheSize = 100, array $allowedProtocolVersions = \Cassandra\Protocol\ProtocolVersion::PREFRED_ORDER, \Cassandra\Protocol\ProtocolVersion $initialProtocolVersion = \Cassandra\Protocol\ProtocolVersion::V4, ?float $requestTimeoutInSeconds = 30, int $maxOrphanedStreams = 24, ?float $heartbeatIntervalInSeconds = 30, float $heartbeatTimeoutInSeconds = 5): mixed
```

**Parameters:**

| Parameter                     | Type                                            | Description |
|-------------------------------|-------------------------------------------------|-------------|
| `$enableCompression`          | **bool**                                        |             |
| `$throwOnOverload`            | **bool**                                        |             |
| `$nodeSelectionStrategy`      | **\Cassandra\Connection\NodeSelectionStrategy** |             |
| `$preparedResultCacheSize`    | **int**                                         |             |
| `$allowedProtocolVersions`    | **array**                                       |             |
| `$initialProtocolVersion`     | **\Cassandra\Protocol\ProtocolVersion**         |             |
| `$requestTimeoutInSeconds`    | **?float**                                      |             |
| `$maxOrphanedStreams`         | **int**                                         |             |
| `$heartbeatIntervalInSeconds` | **?float**                                      |             |
| `$heartbeatTimeoutInSeconds`  | **float**                                       |             |

***

### asStartupOptions

```php
public asStartupOptions(): array<string,string>
```

***

### toArray

```php
public toArray(): array<string,string>
```

* **Warning:** this method is **deprecated**. This means that this method will likely be removed in a future version.
***
