# FunctionFailureContext

***

* Full name: `\Cassandra\Response\Error\Context\FunctionFailureContext`
* Parent class: [`\Cassandra\Response\Error\Context\ErrorContext`](./ErrorContext.md)
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Properties

### keyspace

```php
public string $keyspace
```

***

### function

```php
public string $function
```

***

### argTypes

```php
public array $argTypes
```

***

## Methods

### __construct

```php
public __construct(string $keyspace, string $function, string[] $argTypes): mixed
```

**Parameters:**

| Parameter   | Type         | Description |
|-------------|--------------|-------------|
| `$keyspace` | **string**   |             |
| `$function` | **string**   |             |
| `$argTypes` | **string[]** |             |

***

### toArray

```php
public toArray(): array{keyspace: string, function: string, arg_types: string[]}
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
