# ResponseReader

***

* Full name: `\Cassandra\Connection\ResponseReader`
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Methods

### __construct

```php
public __construct(): mixed
```

***

### readResponse

```php
public readResponse(\Cassandra\Connection\Node $node, \Cassandra\Protocol\ProtocolVersion $version, bool $waitForResponse): ?\Cassandra\Response\Response
```

**Parameters:**

| Parameter          | Type                                    | Description |
|--------------------|-----------------------------------------|-------------|
| `$node`            | **\Cassandra\Connection\Node**          |             |
| `$version`         | **\Cassandra\Protocol\ProtocolVersion** |             |
| `$waitForResponse` | **bool**                                |             |

**Throws:**

- [`NodeException`](../Exception/NodeException.md)
- [`ConnectionException`](../Exception/ConnectionException.md)
- [`ResponseException`](../Exception/ResponseException.md)
- [`CompressionException`](../Exception/CompressionException.md)

***
