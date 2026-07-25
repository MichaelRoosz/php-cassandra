# PreparedData

***

* Full name: `\Cassandra\Response\Result\Data\PreparedData`
* Parent class: [`\Cassandra\Response\Result\Data\ResultData`](./ResultData.md)
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Properties

### id

```php
public string $id
```

***

### prepareMetadata

```php
public \Cassandra\Response\Result\PrepareMetadata $prepareMetadata
```

***

### rowsMetadata

```php
public \Cassandra\Response\Result\RowsMetadata $rowsMetadata
```

***

### rowsMetadataId

```php
public ?string $rowsMetadataId
```

***

## Methods

### __construct

```php
public __construct(string $id, \Cassandra\Response\Result\PrepareMetadata $prepareMetadata, \Cassandra\Response\Result\RowsMetadata $rowsMetadata, ?string $rowsMetadataId = null): mixed
```

**Parameters:**

| Parameter          | Type                                           | Description |
|--------------------|------------------------------------------------|-------------|
| `$id`              | **string**                                     |             |
| `$prepareMetadata` | **\Cassandra\Response\Result\PrepareMetadata** |             |
| `$rowsMetadata`    | **\Cassandra\Response\Result\RowsMetadata**    |             |
| `$rowsMetadataId`  | **?string**                                    |             |

***

## Inherited methods

### __construct

```php
public __construct(): mixed
```

***
