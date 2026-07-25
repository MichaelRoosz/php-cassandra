# Supported

***

* Full name: `\Cassandra\Response\Supported`
* Parent class: [`\Cassandra\Response\Response`](./Response.md)
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Methods

### getData

```php
public getData(): array<string,string[]>
```

**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

***

## Inherited methods

### __construct

```php
public __construct(\Cassandra\Protocol\Header $header, \Cassandra\Response\StreamReader $stream): mixed
```

**Parameters:**

| Parameter | Type                                 | Description |
|-----------|--------------------------------------|-------------|
| `$header` | **\Cassandra\Protocol\Header**       |             |
| `$stream` | **\Cassandra\Response\StreamReader** |             |

**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

***

### __toString

```php
public __toString(): string
```

***

### getBody

```php
public getBody(): string
```

***

### getBodyStreamReader

```php
public getBodyStreamReader(): \Cassandra\Response\StreamReader
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
public getPayload(): ?array<string,?string>
```

***

### getProtocolVersion

```php
public getProtocolVersion(): \Cassandra\Protocol\ProtocolVersion
```

***

### getResponseClassMap

```php
public static getResponseClassMap(): array<int,class-string<\Cassandra\Response\Response>>
```

* This method is **static**.
***

### getStream

The stream id this frame belongs to, or null for a request that has not
been assigned one yet. A response always has one, so
{@see \Cassandra\Response\Response::getStream()} narrows this to int.

```php
public getStream(): int
```

***

### getTracingUuid

```php
public getTracingUuid(): ?string
```

***

### getVersion

```php
public getVersion(): int
```

* **Warning:** this method is **deprecated**. This means that this method will likely be removed in a future version.
***

### getWarnings

```php
public getWarnings(): string[]
```

***

### hasWarnings

```php
public hasWarnings(): bool
```

***
