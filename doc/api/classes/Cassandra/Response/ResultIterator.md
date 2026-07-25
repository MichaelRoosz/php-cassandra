# ResultIterator

***

* Full name: `\Cassandra\Response\ResultIterator`
* This class is marked as **final** and can't be subclassed
* This class implements:
  `Iterator`
* This class is a **Final class**

## Methods

### __construct

```php
public __construct(\Cassandra\Response\Result\RowsResult $rowsResult): mixed
```

**Parameters:**

| Parameter     | Type                                      | Description |
|---------------|-------------------------------------------|-------------|
| `$rowsResult` | **\Cassandra\Response\Result\RowsResult** |             |

***

### current

```php
public current(): \Cassandra\Response\Result\RowClassInterface|array<array-key,mixed>|false
```

**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)
- [`ValueException`](../Exception/ValueException.md)
- [`ValueFactoryException`](../Exception/ValueFactoryException.md)

***

### key

The current position in this result set

```php
public key(): int
```

***

### next

Move forward to next element

```php
public next(): void
```

**Throws:**

- [`ResponseException`](../Exception/ResponseException.md)
- [`ValueException`](../Exception/ValueException.md)
- [`ValueFactoryException`](../Exception/ValueFactoryException.md)

***

### rewind

Reset the result set

```php
public rewind(): void
```

***

### valid

Checks if current position is valid

```php
public valid(): bool
```

***
