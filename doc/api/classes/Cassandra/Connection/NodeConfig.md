# NodeConfig

***

* Full name: `\Cassandra\Connection\NodeConfig`
* This class is an **Abstract class**

## Properties

### host

```php
public string $host
```

***

### port

```php
public int $port
```

***

### username

```php
public string $username
```

***

### password

```php
public string $password
```

***

## Methods

### __construct

```php
public __construct(string $host = 'localhost', int $port = 9042, string $username = '', string $password = ''): mixed
```

**Parameters:**

| Parameter   | Type       | Description |
|-------------|------------|-------------|
| `$host`     | **string** |             |
| `$port`     | **int**    |             |
| `$username` | **string** |             |
| `$password` | **string** |             |

***

### getNodeClass

```php
public getNodeClass(): class-string<\Cassandra\Connection\IoNode>
```

* This method is **abstract**.
***
