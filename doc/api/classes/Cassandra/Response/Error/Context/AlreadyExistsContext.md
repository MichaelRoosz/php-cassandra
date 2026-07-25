# AlreadyExistsContext

***

* Full name: `\Cassandra\Response\Error\Context\AlreadyExistsContext`
* Parent class: [`\Cassandra\Response\Error\Context\ErrorContext`](./ErrorContext.md)
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Properties

### keyspace

```php
public string $keyspace
```

***

### table

```php
public string $table
```

***

## Methods

### __construct

```php
public __construct(string $keyspace, string $table): mixed
```

**Parameters:**

| Parameter   | Type       | Description |
|-------------|------------|-------------|
| `$keyspace` | **string** |             |
| `$table`    | **string** |             |

***

### toArray

```php
public toArray(): array{keyspace: string, table: string}
```

***

## Inherited methods

### __construct

```php
public __construct(): mixed
```

***

### toArray

```php
public toArray(): array<string,mixed>
```

***
