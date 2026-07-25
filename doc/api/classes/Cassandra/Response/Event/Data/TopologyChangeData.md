# TopologyChangeData

***

* Full name: `\Cassandra\Response\Event\Data\TopologyChangeData`
* Parent class: [`\Cassandra\Response\Event\Data\EventData`](./EventData.md)
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Properties

### changeType

```php
public \Cassandra\Response\Event\Data\TopologyChangeType $changeType
```

***

### address

```php
public array{ip: string, port: int} $address
```

***

## Methods

### __construct

```php
public __construct(\Cassandra\Response\Event\Data\TopologyChangeType $changeType, array $address): mixed
```

**Parameters:**

| Parameter     | Type                                                  | Description |
|---------------|-------------------------------------------------------|-------------|
| `$changeType` | **\Cassandra\Response\Event\Data\TopologyChangeType** |             |
| `$address`    | **array**                                             |             |

***

## Inherited methods

### __construct

```php
public __construct(): mixed
```

***
