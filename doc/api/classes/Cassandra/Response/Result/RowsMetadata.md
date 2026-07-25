# RowsMetadata

***

* Full name: `\Cassandra\Response\Result\RowsMetadata`
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Properties

### flags

```php
public int $flags
```

***

### columnsCount

```php
public int $columnsCount
```

***

### pagingState

```php
public ?string $pagingState
```

***

### metadataId

```php
public ?string $metadataId
```

***

### columns

```php
public ?\Cassandra\Response\Result\ColumnInfo[] $columns
```

***

## Methods

### __construct

```php
public __construct(int $flags, int $columnsCount, ?string $pagingState, ?string $metadataId, ?array $columns): mixed
```

**Parameters:**

| Parameter       | Type        | Description |
|-----------------|-------------|-------------|
| `$flags`        | **int**     |             |
| `$columnsCount` | **int**     |             |
| `$pagingState`  | **?string** |             |
| `$metadataId`   | **?string** |             |
| `$columns`      | **?array**  |             |

***

### mergeWithPreviousMetadata

```php
public mergeWithPreviousMetadata(\Cassandra\Response\Result\RowsMetadata $previousMetadata): self
```

**Parameters:**

| Parameter           | Type                                        | Description |
|---------------------|---------------------------------------------|-------------|
| `$previousMetadata` | **\Cassandra\Response\Result\RowsMetadata** |             |

***

### withMetadataId

```php
public withMetadataId(?string $metadataId): self
```

**Parameters:**

| Parameter     | Type        | Description |
|---------------|-------------|-------------|
| `$metadataId` | **?string** |             |

***
