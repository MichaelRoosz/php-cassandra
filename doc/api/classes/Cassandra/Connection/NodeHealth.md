# NodeHealth

***

* Full name: `\Cassandra\Connection\NodeHealth`
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Methods

### isAvailable

```php
public isAvailable(\Cassandra\Connection\NodeConfig $config): bool
```

**Parameters:**

| Parameter | Type                                 | Description |
|-----------|--------------------------------------|-------------|
| `$config` | **\Cassandra\Connection\NodeConfig** |             |

***

### partitionByAvailability

```php
public partitionByAvailability(\Cassandra\Connection\NodeConfig[] $nodes): array{available: \Cassandra\Connection\NodeConfig[], unavailable: \Cassandra\Connection\NodeConfig[]}
```

**Parameters:**

| Parameter | Type                                   | Description |
|-----------|----------------------------------------|-------------|
| `$nodes`  | **\Cassandra\Connection\NodeConfig[]** |             |

***

### recordFailure

```php
public recordFailure(\Cassandra\Connection\NodeConfig $config): void
```

**Parameters:**

| Parameter | Type                                 | Description |
|-----------|--------------------------------------|-------------|
| `$config` | **\Cassandra\Connection\NodeConfig** |             |

***

### recordSuccess

```php
public recordSuccess(\Cassandra\Connection\NodeConfig $config): void
```

**Parameters:**

| Parameter | Type                                 | Description |
|-----------|--------------------------------------|-------------|
| `$config` | **\Cassandra\Connection\NodeConfig** |             |

***
