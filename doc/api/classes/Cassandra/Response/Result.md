# Result

***

* Full name: `\Cassandra\Response\Result`
* Parent class: [`\Cassandra\Response\Response`](./Response.md)
* This class implements:
  `IteratorAggregate`

## Methods

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

### asPreparedResult

```php
public asPreparedResult(): \Cassandra\Response\Result\PreparedResult
```

**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

***

### asRowsResult

```php
public asRowsResult(): \Cassandra\Response\Result\RowsResult
```

**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

***

### asSchemaChangeResult

```php
public asSchemaChangeResult(): \Cassandra\Response\Result\SchemaChangeResult
```

**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

***

### asSetKeyspaceResult

```php
public asSetKeyspaceResult(): \Cassandra\Response\Result\SetKeyspaceResult
```

**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

***

### asVoidResult

```php
public asVoidResult(): \Cassandra\Response\Result\VoidResult
```

**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

***

### getIterator

```php
public getIterator(): \Iterator
```

***

### getKind

```php
public getKind(): \Cassandra\Response\ResultKind
```

***

### getLastPreparedData

```php
public getLastPreparedData(): ?\Cassandra\Response\Result\Data\PreparedData
```

***

### getRequest

```php
public getRequest(): ?\Cassandra\Request\Request
```

***

### getResultClassMap

```php
public static getResultClassMap(): array<int,class-string<\Cassandra\Response\Result>>
```

* This method is **static**.
***

### getRowCount

```php
public getRowCount(): int
```

***

### setPreviousResult

```php
public setPreviousResult(\Cassandra\Response\Result $previousResult): static
```

**Parameters:**

| Parameter         | Type                           | Description |
|-------------------|--------------------------------|-------------|
| `$previousResult` | **\Cassandra\Response\Result** |             |

**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)

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
