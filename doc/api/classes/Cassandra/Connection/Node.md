# Node

***

* Full name: `\Cassandra\Connection\Node`

## Methods

### close

```php
public close(): void
```

***

### getConfig

```php
public getConfig(): \Cassandra\Connection\NodeConfig
```

***

### read

Returns exactly $length bytes of data, or an empty string if not enough data is available.

```php
public read(int $length, bool $waitForData): string
```

If $waitForData is true, it will block until $length bytes are available.

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

**Parameters:**

| Parameter  | Type                           | Description |
|------------|--------------------------------|-------------|
| `$request` | **\Cassandra\Request\Request** |             |

**Throws:**

- [`NodeException`](../Exception/NodeException.md)

***
