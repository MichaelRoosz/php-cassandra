# BatchOptions

***

* Full name: `\Cassandra\Request\Options\BatchOptions`
* Parent class: [`\Cassandra\Request\Options\RequestOptions`](./RequestOptions.md)
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Properties

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
public __construct(?\Cassandra\SerialConsistency $serialConsistency = null, ?int $defaultTimestamp = null, ?string $keyspace = null, ?int $nowInSeconds = null, ?float $requestTimeoutInSeconds = null): mixed
```

**Parameters:**

| Parameter                  | Type                              | Description |
|----------------------------|-----------------------------------|-------------|
| `$serialConsistency`       | **?\Cassandra\SerialConsistency** |             |
| `$defaultTimestamp`        | **?int**                          |             |
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
