# PrepareOptions

***

* Full name: `\Cassandra\Request\Options\PrepareOptions`
* Parent class: [`\Cassandra\Request\Options\RequestOptions`](./RequestOptions.md)
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Properties

### keyspace

```php
public ?string $keyspace
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
public __construct(?string $keyspace = null, ?float $requestTimeoutInSeconds = null): mixed
```

**Parameters:**

| Parameter                  | Type        | Description |
|----------------------------|-------------|-------------|
| `$keyspace`                | **?string** |             |
| `$requestTimeoutInSeconds` | **?float**  |             |

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
