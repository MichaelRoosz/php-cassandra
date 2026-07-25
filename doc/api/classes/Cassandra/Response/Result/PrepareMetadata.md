# PrepareMetadata

***

* Full name: `\Cassandra\Response\Result\PrepareMetadata`
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Properties

### flags

```php
public int $flags
```

***

### bindMarkersCount

```php
public int $bindMarkersCount
```

***

### bindMarkers

```php
public \Cassandra\Response\Result\ColumnInfo[] $bindMarkers
```

***

### pkCount

```php
public ?int $pkCount
```

***

### pkIndex

```php
public int[]|null $pkIndex
```

***

## Methods

### __construct

```php
public __construct(int $flags, int $bindMarkersCount, array $bindMarkers, ?int $pkCount, ?array $pkIndex): mixed
```

**Parameters:**

| Parameter           | Type       | Description |
|---------------------|------------|-------------|
| `$flags`            | **int**    |             |
| `$bindMarkersCount` | **int**    |             |
| `$bindMarkers`      | **array**  |             |
| `$pkCount`          | **?int**   |             |
| `$pkIndex`          | **?array** |             |

***
