# Prepare

***

* Full name: `\Cassandra\Request\Prepare`
* Parent class: [`\Cassandra\Request\Request`](./Request.md)
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Methods

### __construct

```php
public __construct(string $query, \Cassandra\Request\Options\PrepareOptions $options = new \Cassandra\Request\Options\PrepareOptions()): mixed
```

**Parameters:**

| Parameter  | Type                                          | Description |
|------------|-----------------------------------------------|-------------|
| `$query`   | **string**                                    |             |
| `$options` | **\Cassandra\Request\Options\PrepareOptions** |             |

***

### getBody

```php
public getBody(): string
```

**Throws:**

- [`RequestException`](../Exception/RequestException.md)

***

### getHash

```php
public getHash(): string
```

***

### getOptions

```php
public getOptions(): \Cassandra\Request\Options\PrepareOptions
```

***

### getQuery

```php
public getQuery(): string
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
