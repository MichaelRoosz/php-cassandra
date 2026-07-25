# QueryOptions

***

* Full name: `\Cassandra\Request\Options\QueryOptions`
* Parent class: [`\Cassandra\Request\Options\RequestOptions`](./RequestOptions.md)

## Properties

### autoPrepare

```php
public bool $autoPrepare
```

***

### pageSize

```php
public ?int $pageSize
```

***

### pagingState

```php
public ?string $pagingState
```

***

### serialConsistency

```php
public ?\Cassandra\SerialConsistency $serialConsistency
```

***

### defaultTimestamp

```php
public ?int $defaultTimestamp
```

***

### namesForValues

```php
public ?bool $namesForValues
```

***

### keyspace

```php
public ?string $keyspace
```

***

### nowInSeconds

```php
public ?int $nowInSeconds
```

***

### requestTimeoutInSeconds

How long to wait for the server to answer this request, in seconds,
overriding the connection default. Null uses the connection default.

```php
public ?float $requestTimeoutInSeconds
```

***

## Methods

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
