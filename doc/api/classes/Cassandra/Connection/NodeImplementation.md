# NodeImplementation

***

* Full name: `\Cassandra\Connection\NodeImplementation`
* This class implements:
  [`\Cassandra\Connection\Node`](./Node.md)
* This class is an **Abstract class**

## Methods

### close

```php
public close(): void
```

* This method is **abstract**.
***

### getConfig

```php
public getConfig(): \Cassandra\Connection\NodeConfig
```

* This method is **abstract**.
***

### read

Returns exactly $length bytes of data, or an empty string if not enough data is available.

```php
public read(int $length, bool $waitForData): string
```

If $waitForData is true this blocks until the data source yields something, but a single
call still performs a single read: a short read (the peer sent only part of what we asked
for) returns an empty string. Whatever arrived stays buffered, so callers that need all
$length bytes must call again until they get a non-empty result.

**Parameters:**

| Parameter      | Type     | Description |
|----------------|----------|-------------|
| `$length`      | **int**  |             |
| `$waitForData` | **bool** |             |

**Throws:**

- [`NodeException`](../Exception/NodeException.md)

***

### readAvailableData

Returns up to $maxLength bytes of data, or an empty string if no data is available.

```php
public readAvailableData(int $expectedLength, int $maxLength, bool $waitForData): string
```

If $waitForData is true, it will block until at least one byte is available.

**Parameters:**

| Parameter         | Type     | Description |
|-------------------|----------|-------------|
| `$expectedLength` | **int**  |             |
| `$maxLength`      | **int**  |             |
| `$waitForData`    | **bool** |             |

**Throws:**

- [`NodeException`](../Exception/NodeException.md)

***

### readAvailableDataFromSource

Returns some bytes of data, or an empty string if no data is available.

```php
public readAvailableDataFromSource(int $expectedLength, int $upperBoundaryLength, bool $waitForData): string
```

$upperBoundaryLength marks an upper boundary for the amount of data that will be returned, but more or less data may be returned.
If $waitForData is true, it will block until at least one byte is available.

* This method is **abstract**.
**Parameters:**

| Parameter              | Type     | Description |
|------------------------|----------|-------------|
| `$expectedLength`      | **int**  |             |
| `$upperBoundaryLength` | **int**  |             |
| `$waitForData`         | **bool** |             |

**Throws:**

- [`NodeException`](../Exception/NodeException.md)

***

### write

```php
public write(string $data): void
```

* This method is **abstract**.
**Parameters:**

| Parameter | Type       | Description |
|-----------|------------|-------------|
| `$data`   | **string** |             |

**Throws:**

- [`NodeException`](../Exception/NodeException.md)

***

### writeRequest

```php
public writeRequest(\Cassandra\Request\Request $request): void
```

* This method is **abstract**.
**Parameters:**

| Parameter  | Type                           | Description |
|------------|--------------------------------|-------------|
| `$request` | **\Cassandra\Request\Request** |             |

**Throws:**

- [`NodeException`](../Exception/NodeException.md)

***
