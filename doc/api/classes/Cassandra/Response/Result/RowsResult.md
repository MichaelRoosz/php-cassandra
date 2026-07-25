# RowsResult

***

* Full name: `\Cassandra\Response\Result\RowsResult`
* Parent class: [`\Cassandra\Response\Result`](../Result.md)
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Methods

### __construct

```php
final public __construct(\Cassandra\Protocol\Header $header, \Cassandra\Response\StreamReader $stream): mixed
```

* This method is **final**.
**Parameters:**

| Parameter | Type                                 | Description |
|-----------|--------------------------------------|-------------|
| `$header` | **\Cassandra\Protocol\Header**       |             |
| `$stream` | **\Cassandra\Response\StreamReader** |             |

**Throws:**

- [`ResponseException`](../../Exception/ResponseException.md)
- [`ValueException`](../../Exception/ValueException.md)
- [`TypeNameParserException`](../../Exception/TypeNameParserException.md)

***

### columnCount

```php
public columnCount(): int
```

***

### configureFetchObject

```php
public configureFetchObject(class-string<\Cassandra\Response\Result\RowClassInterface> $rowClass, array $constructorArgs = [], \Cassandra\Response\Result\FetchType $fetchType = \Cassandra\Response\Result\FetchType::ASSOC): void
```

**Parameters:**

| Parameter          | Type                                                           | Description |
|--------------------|----------------------------------------------------------------|-------------|
| `$rowClass`        | **class-string<\Cassandra\Response\Result\RowClassInterface>** |             |
| `$constructorArgs` | **array**                                                      |             |
| `$fetchType`       | **\Cassandra\Response\Result\FetchType**                       |             |

**Throws:**

- [`ResponseException`](../../Exception/ResponseException.md)

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

### fetch

Fetches the next row from the result set.

```php
public fetch(\Cassandra\Response\Result\FetchType $mode = \Cassandra\Response\Result\FetchType::ASSOC): array<string|int,mixed>|false
```

**Parameters:**

| Parameter | Type                                     | Description |
|-----------|------------------------------------------|-------------|
| `$mode`   | **\Cassandra\Response\Result\FetchType** |             |

**Throws:**

- [`ResponseException`](../../Exception/ResponseException.md)
- [`ValueException`](../../Exception/ValueException.md)
- [`ValueFactoryException`](../../Exception/ValueFactoryException.md)

***

### fetchAll

Fetches the remaining rows from the current cursor position.

```php
public fetchAll(\Cassandra\Response\Result\FetchType $mode = \Cassandra\Response\Result\FetchType::ASSOC): array<int,array<string|int,mixed>>
```

**Parameters:**

| Parameter | Type                                     | Description |
|-----------|------------------------------------------|-------------|
| `$mode`   | **\Cassandra\Response\Result\FetchType** |             |

**Throws:**

- [`ResponseException`](../../Exception/ResponseException.md)
- [`ValueException`](../../Exception/ValueException.md)
- [`ValueFactoryException`](../../Exception/ValueFactoryException.md)

***

### fetchAllColumns

Fetches the remaining rows from the current cursor position and returns
the value of the specified column for each row. Behaves like fetchAll()
in that it consumes the stream from the current cursor forward.

```php
public fetchAllColumns(int $index = 0): array
```

**Parameters:**

| Parameter | Type    | Description |
|-----------|---------|-------------|
| `$index`  | **int** |             |

**Throws:**

- [`ResponseException`](../../Exception/ResponseException.md)
- [`ValueException`](../../Exception/ValueException.md)
- [`ValueFactoryException`](../../Exception/ValueFactoryException.md)

***

### fetchAllKeyPairs

Fetches remaining rows and returns an associative map of key => value.

```php
public fetchAllKeyPairs(int $keyIndex = 0, int $valueIndex = 1, bool $mergeDuplicates = false): array<int|string,mixed>
```

Consumes the cursor from the current position forward.

**Parameters:**

| Parameter          | Type     | Description |
|--------------------|----------|-------------|
| `$keyIndex`        | **int**  |             |
| `$valueIndex`      | **int**  |             |
| `$mergeDuplicates` | **bool** |             |

**Throws:**

- [`ResponseException`](../../Exception/ResponseException.md)
- [`ValueException`](../../Exception/ValueException.md)
- [`ValueFactoryException`](../../Exception/ValueFactoryException.md)

***

### fetchAllObjects

Fetches all remaining rows and returns them as RowClassInterface instances.

```php
public fetchAllObjects(): \Cassandra\Response\Result\RowClassInterface[]
```

**Throws:**

- [`ResponseException`](../../Exception/ResponseException.md)
- [`ValueException`](../../Exception/ValueException.md)
- [`ValueFactoryException`](../../Exception/ValueFactoryException.md)

***

### fetchColumn

Returns a single column from the next row of a result set.

```php
public fetchColumn(int $index = 0): mixed|false
```

Returns false when there are no more rows.

**Parameters:**

| Parameter | Type    | Description |
|-----------|---------|-------------|
| `$index`  | **int** |             |

**Throws:**

- [`ResponseException`](../../Exception/ResponseException.md)
- [`ValueException`](../../Exception/ValueException.md)
- [`ValueFactoryException`](../../Exception/ValueFactoryException.md)

***

### fetchKeyPair

Fetches a single key/value pair from the next row.

```php
public fetchKeyPair(int $keyIndex = 0, int $valueIndex = 1): array<int|string,mixed>|false
```

Returns false when there are no more rows.

**Parameters:**

| Parameter     | Type    | Description |
|---------------|---------|-------------|
| `$keyIndex`   | **int** |             |
| `$valueIndex` | **int** |             |

**Throws:**

- [`ResponseException`](../../Exception/ResponseException.md)
- [`ValueException`](../../Exception/ValueException.md)
- [`ValueFactoryException`](../../Exception/ValueFactoryException.md)

***

### fetchObject

Fetches the next row and returns it as an RowClassInterface instance.

```php
public fetchObject(): \Cassandra\Response\Result\RowClassInterface|false
```

Returns false when there are no more rows.

**Throws:**

- [`ResponseException`](../../Exception/ResponseException.md)
- [`ValueException`](../../Exception/ValueException.md)
- [`ValueFactoryException`](../../Exception/ValueFactoryException.md)

***

### getData

```php
public getData(): \Cassandra\Response\Result\Data\ResultData
```

**Throws:**

- [`ResponseException`](../../Exception/ResponseException.md)
- [`ValueException`](../../Exception/ValueException.md)
- [`ValueFactoryException`](../../Exception/ValueFactoryException.md)

***

### getIterator

```php
public getIterator(): \Cassandra\Response\ResultIterator
```

***

### getRowCount

```php
public getRowCount(): int
```

***

### getRowsData

```php
public getRowsData(): \Cassandra\Response\Result\Data\RowsData
```

**Throws:**

- [`ResponseException`](../../Exception/ResponseException.md)
- [`ValueException`](../../Exception/ValueException.md)
- [`ValueFactoryException`](../../Exception/ValueFactoryException.md)

***

### getRowsMetadata

```php
public getRowsMetadata(): \Cassandra\Response\Result\RowsMetadata
```

***

### hasMetadataChanged

```php
public hasMetadataChanged(): bool
```

***

### hasMorePages

```php
public hasMorePages(): bool
```

***

### hasNoMetadata

```php
public hasNoMetadata(): bool
```

***

### isFetchObjectConfigurationSet

```php
public isFetchObjectConfigurationSet(): bool
```

***

### resetFetchObjectConfiguration

```php
public resetFetchObjectConfiguration(): void
```

***

### rewind

```php
public rewind(): void
```

***

### rewindOneRow

```php
public rewindOneRow(): void
```

***

### rowCount

```php
public rowCount(): int
```

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

- [`ResponseException`](../../Exception/ResponseException.md)

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

### asPreparedResult

```php
public asPreparedResult(): \Cassandra\Response\Result\PreparedResult
```

**Throws:**

- [`ResponseException`](../../Exception/ResponseException.md)

***

### asRowsResult

```php
public asRowsResult(): \Cassandra\Response\Result\RowsResult
```

**Throws:**

- [`ResponseException`](../../Exception/ResponseException.md)

***

### asSchemaChangeResult

```php
public asSchemaChangeResult(): \Cassandra\Response\Result\SchemaChangeResult
```

**Throws:**

- [`ResponseException`](../../Exception/ResponseException.md)

***

### asSetKeyspaceResult

```php
public asSetKeyspaceResult(): \Cassandra\Response\Result\SetKeyspaceResult
```

**Throws:**

- [`ResponseException`](../../Exception/ResponseException.md)

***

### asVoidResult

```php
public asVoidResult(): \Cassandra\Response\Result\VoidResult
```

**Throws:**

- [`ResponseException`](../../Exception/ResponseException.md)

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

- [`ResponseException`](../../Exception/ResponseException.md)

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
