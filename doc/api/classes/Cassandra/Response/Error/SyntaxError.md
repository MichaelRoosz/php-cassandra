# SyntaxError

Indicates an error processing a request.

***

* Full name: `\Cassandra\Response\Error\SyntaxError`
* Parent class: [`\Cassandra\Response\Error`](../Error.md)
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

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

### getCode

```php
public getCode(): int
```

***

### getContext

```php
public getContext(): \Cassandra\Response\Error\Context\ErrorContext
```

***

### getErrorClassMap

```php
public static getErrorClassMap(): array<int,class-string<\Cassandra\Response\Error>>
```

* This method is **static**.
***

### getException

```php
public getException(): \Cassandra\Exception\ServerException
```

***

### getMessage

```php
public getMessage(): string
```

***

### getType

```php
public getType(): \Cassandra\Response\ErrorType
```

***
