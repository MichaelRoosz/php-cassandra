# ExecuteOptions

***

* Full name: `\Cassandra\Request\Options\ExecuteOptions`
* Parent class: [`\Cassandra\Request\Options\QueryOptions`](./QueryOptions.md)
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Properties

### skipMetadata

```php
public ?bool $skipMetadata
```

***

## Methods

### __construct

```php
public __construct(?bool $skipMetadata = null, ?int $pageSize = null, ?string $pagingState = null, ?\Cassandra\SerialConsistency $serialConsistency = null, ?int $defaultTimestamp = null, ?bool $namesForValues = null, ?string $keyspace = null, ?int $nowInSeconds = null, ?float $requestTimeoutInSeconds = null): mixed
```

**Parameters:**

| Parameter                  | Type                              | Description |
|----------------------------|-----------------------------------|-------------|
| `$skipMetadata`            | **?bool**                         |             |
| `$pageSize`                | **?int**                          |             |
| `$pagingState`             | **?string**                       |             |
| `$serialConsistency`       | **?\Cassandra\SerialConsistency** |             |
| `$defaultTimestamp`        | **?int**                          |             |
| `$namesForValues`          | **?bool**                         |             |
| `$keyspace`                | **?string**                       |             |
| `$nowInSeconds`            | **?int**                          |             |
| `$requestTimeoutInSeconds` | **?float**                        |             |

***

### fromQueryOptions

```php
public static fromQueryOptions(\Cassandra\Request\Options\QueryOptions $options, ?bool $skipMetadata = null): self
```

* This method is **static**.
**Parameters:**

| Parameter       | Type                                        | Description |
|-----------------|---------------------------------------------|-------------|
| `$options`      | **\Cassandra\Request\Options\QueryOptions** |             |
| `$skipMetadata` | **?bool**                                   |             |

***

### withKeyspace

```php
public withKeyspace(string $keyspace): self
```

**Parameters:**

| Parameter   | Type       | Description |
|-------------|------------|-------------|
| `$keyspace` | **string** |             |

***

### withNamesForValues

```php
public withNamesForValues(bool $namesForValues): self
```

**Parameters:**

| Parameter         | Type     | Description |
|-------------------|----------|-------------|
| `$namesForValues` | **bool** |             |

***

### withPagingState

```php
public withPagingState(string $pagingState): self
```

**Parameters:**

| Parameter      | Type       | Description |
|----------------|------------|-------------|
| `$pagingState` | **string** |             |

***

### withSkipMetadata

```php
public withSkipMetadata(bool $skipMetadata): self
```

**Parameters:**

| Parameter       | Type     | Description |
|-----------------|----------|-------------|
| `$skipMetadata` | **bool** |             |

***

## Inherited methods

### __construct

```php
public __construct(bool $autoPrepare = true, ?int $pageSize = null, ?string $pagingState = null, ?\Cassandra\SerialConsistency $serialConsistency = null, ?int $defaultTimestamp = null, ?bool $namesForValues = null, ?string $keyspace = null, ?int $nowInSeconds = null, ?float $requestTimeoutInSeconds = null): mixed
```

**Parameters:**

| Parameter                  | Type                              | Description |
|----------------------------|-----------------------------------|-------------|
| `$autoPrepare`             | **bool**                          |             |
| `$pageSize`                | **?int**                          |             |
| `$pagingState`             | **?string**                       |             |
| `$serialConsistency`       | **?\Cassandra\SerialConsistency** |             |
| `$defaultTimestamp`        | **?int**                          |             |
| `$namesForValues`          | **?bool**                         |             |
| `$keyspace`                | **?string**                       |             |
| `$nowInSeconds`            | **?int**                          |             |
| `$requestTimeoutInSeconds` | **?float**                        |             |

***

### withKeyspace

```php
public withKeyspace(string $keyspace): self
```

**Parameters:**

| Parameter   | Type       | Description |
|-------------|------------|-------------|
| `$keyspace` | **string** |             |

***

### withNamesForValues

```php
public withNamesForValues(bool $namesForValues): self
```

**Parameters:**

| Parameter         | Type     | Description |
|-------------------|----------|-------------|
| `$namesForValues` | **bool** |             |

***

### withPagingState

```php
public withPagingState(string $pagingState): self
```

**Parameters:**

| Parameter      | Type       | Description |
|----------------|------------|-------------|
| `$pagingState` | **string** |             |

***
