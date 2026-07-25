# Startup

***

* Full name: `\Cassandra\Request\Startup`
* Parent class: [`\Cassandra\Request\Request`](./Request.md)
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Methods

### __construct

STARTUP

```php
public __construct(array<string,string> $options = []): mixed
```

Initialize the connection. The server will respond by either a READY message
(in which case the connection is ready for queries) or an AUTHENTICATE message
(in which case credentials will need to be provided using CREDENTIALS).

This must be the first message of the connection, except for OPTIONS that can
be sent before to find out the options supported by the server. Once the
connection has been initialized, a client should not send any more STARTUP
message.

Possible options are:
- "CQL_VERSION": the version of CQL to use. This option is mandatory and
currenty, the only version supported is "3.0.0". Note that this is
different from the protocol version.
- "COMPRESSION": the compression algorithm to use for frames (See section 5).
This is optional, if not specified no compression will be used.

**Parameters:**

| Parameter  | Type                     | Description |
|------------|--------------------------|-------------|
| `$options` | **array<string,string>** |             |

***

### getBody

```php
public getBody(): string
```

***

## Inherited methods

### __construct

```php
public __construct(\Cassandra\Protocol\Opcode $opcode, ?int $stream = null, int $flags = 0, ?array<string,string> $payload = null, \Cassandra\Protocol\ProtocolVersion $version = \Cassandra\Protocol\ProtocolVersion::V3): mixed
```

**Parameters:**

| Parameter  | Type                                    | Description |
|------------|-----------------------------------------|-------------|
| `$opcode`  | **\Cassandra\Protocol\Opcode**          |             |
| `$stream`  | **?int**                                |             |
| `$flags`   | **int**                                 |             |
| `$payload` | **?array<string,string>**               |             |
| `$version` | **\Cassandra\Protocol\ProtocolVersion** |             |

***

### __toString

```php
public __toString(): string
```

**Throws:**

- [`RequestException`](../Exception/RequestException.md)

***

### enableTracing

```php
public enableTracing(): void
```

***

### getBody

```php
public getBody(): string
```

***

### getFlags

```php
public getFlags(): int
```

***

### getOpcode

```php
public getOpcode(): \Cassandra\Protocol\Opcode
```

***

### getPayload

```php
public getPayload(): ?array<string,string>
```

***

### getProtocolVersion

```php
public getProtocolVersion(): \Cassandra\Protocol\ProtocolVersion
```

***

### getRequestTimeout

How long the server may take to answer this request, if the request asks
for something other than the connection's default, and null otherwise.

```php
public getRequestTimeout(): ?float
```

Requests that carry options override this; the rest — STARTUP, OPTIONS,
REGISTER, AUTH_RESPONSE — have nothing to say about it.

***

### getStream

The stream id this request will be sent on, or null while it has not
been assigned one. Encoding the request is what requires an id, so that
is where an unassigned one is refused.

```php
public getStream(): ?int
```

***

### getVersion

```php
public getVersion(): int
```

* **Warning:** this method is **deprecated**. This means that this method will likely be removed in a future version.
***

### setFlags

```php
public setFlags(int $flags): void
```

**Parameters:**

| Parameter | Type    | Description |
|-----------|---------|-------------|
| `$flags`  | **int** |             |

***

### setPayload

```php
public setPayload(array<string,string> $payload): void
```

**Parameters:**

| Parameter  | Type                     | Description |
|------------|--------------------------|-------------|
| `$payload` | **array<string,string>** |             |

***

### setStream

```php
public setStream(int $stream): void
```

**Parameters:**

| Parameter | Type    | Description |
|-----------|---------|-------------|
| `$stream` | **int** |             |

***

### setVersion

```php
public setVersion(\Cassandra\Protocol\ProtocolVersion $version): void
```

**Parameters:**

| Parameter  | Type                                    | Description |
|------------|-----------------------------------------|-------------|
| `$version` | **\Cassandra\Protocol\ProtocolVersion** |             |

***
