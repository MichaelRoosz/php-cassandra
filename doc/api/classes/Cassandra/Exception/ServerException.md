# ServerException

***

* Full name: `\Cassandra\Exception\ServerException`
* Parent class: [`\Cassandra\Exception\CassandraException`](./CassandraException.md)

## Methods

### __construct

```php
public __construct(\Cassandra\Response\Error\Context\ErrorContext $errorContext, string $message, int $code, array{error_code: int, error_type: string, protocol_version: string, stream_id: int, tracing_uuid: string|null, warnings: string[], payload: array<string,?string>|null} $context, ?\Throwable $previous = null): mixed
```

**Parameters:**

| Parameter       | Type                                                                                                                                                                           | Description |
|-----------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| `$errorContext` | **\Cassandra\Response\Error\Context\ErrorContext**                                                                                                                             |             |
| `$message`      | **string**                                                                                                                                                                     |             |
| `$code`         | **int**                                                                                                                                                                        |             |
| `$context`      | **array{error_code: int, error_type: string, protocol_version: string, stream_id: int, tracing_uuid: string\|null, warnings: string[], payload: array<string,?string>\|null}** |             |
| `$previous`     | **?\Throwable**                                                                                                                                                                |             |

***

### context

```php
public context(): array{error_code: int, error_type: string, protocol_version: string, stream_id: int, tracing_uuid: string|null, warnings: string[], payload: array<string,?string>|null}
```

***

### getContext

```php
public getContext(): array{error_code: int, error_type: string, protocol_version: string, stream_id: int, tracing_uuid: string|null, warnings: string[], payload: array<string,?string>|null}
```

***

### getErrorContext

```php
public getErrorContext(): \Cassandra\Response\Error\Context\ErrorContext
```

***

## Inherited methods

### __construct

```php
public __construct(string $message, int $code, array $context = [], ?\Throwable $previous = null): mixed
```

**Parameters:**

| Parameter   | Type            | Description |
|-------------|-----------------|-------------|
| `$message`  | **string**      |             |
| `$code`     | **int**         |             |
| `$context`  | **array**       |             |
| `$previous` | **?\Throwable** |             |

***

### context

```php
public context(): array
```

**Return Value:**

$context

***

### getContext

```php
public getContext(): array
```

**Return Value:**

$context

***
