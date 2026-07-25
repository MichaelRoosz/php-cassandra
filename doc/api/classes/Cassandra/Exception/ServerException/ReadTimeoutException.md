# ReadTimeoutException

***

* Full name: `\Cassandra\Exception\ServerException\ReadTimeoutException`
* Parent class: [`\Cassandra\Exception\ServerException`](../ServerException.md)
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Methods

### getErrorContext

```php
public getErrorContext(): \Cassandra\Response\Error\Context\ReadTimeoutContext
```

**Throws:**

- [`ResponseException`](../ResponseException.md)

***

## Inherited methods

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
