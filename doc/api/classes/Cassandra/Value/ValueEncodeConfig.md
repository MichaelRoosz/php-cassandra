# ValueEncodeConfig

***

* Full name: `\Cassandra\Value\ValueEncodeConfig`
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Properties

### dateEncodeOption

```php
public \Cassandra\Value\EncodeOption\DateEncodeOption $dateEncodeOption
```

***

### durationEncodeOption

```php
public \Cassandra\Value\EncodeOption\DurationEncodeOption $durationEncodeOption
```

***

### timeEncodeOption

```php
public \Cassandra\Value\EncodeOption\TimeEncodeOption $timeEncodeOption
```

***

### timestampEncodeOption

```php
public \Cassandra\Value\EncodeOption\TimestampEncodeOption $timestampEncodeOption
```

***

### uuidEncodeOption

```php
public \Cassandra\Value\EncodeOption\UuidEncodeOption $uuidEncodeOption
```

***

### varintEncodeOption

```php
public \Cassandra\Value\EncodeOption\VarintEncodeOption $varintEncodeOption
```

***

## Methods

### __construct

```php
public __construct(\Cassandra\Value\EncodeOption\DateEncodeOption $dateEncodeOption = \Cassandra\Value\EncodeOption\DateEncodeOption::AS_STRING, \Cassandra\Value\EncodeOption\DurationEncodeOption $durationEncodeOption = \Cassandra\Value\EncodeOption\DurationEncodeOption::AS_STRING, \Cassandra\Value\EncodeOption\TimeEncodeOption $timeEncodeOption = \Cassandra\Value\EncodeOption\TimeEncodeOption::AS_STRING, \Cassandra\Value\EncodeOption\TimestampEncodeOption $timestampEncodeOption = \Cassandra\Value\EncodeOption\TimestampEncodeOption::AS_STRING, \Cassandra\Value\EncodeOption\UuidEncodeOption $uuidEncodeOption = \Cassandra\Value\EncodeOption\UuidEncodeOption::AS_STRING, \Cassandra\Value\EncodeOption\VarintEncodeOption $varintEncodeOption = \Cassandra\Value\EncodeOption\VarintEncodeOption::AS_STRING): mixed
```

**Parameters:**

| Parameter                | Type                                                    | Description |
|--------------------------|---------------------------------------------------------|-------------|
| `$dateEncodeOption`      | **\Cassandra\Value\EncodeOption\DateEncodeOption**      |             |
| `$durationEncodeOption`  | **\Cassandra\Value\EncodeOption\DurationEncodeOption**  |             |
| `$timeEncodeOption`      | **\Cassandra\Value\EncodeOption\TimeEncodeOption**      |             |
| `$timestampEncodeOption` | **\Cassandra\Value\EncodeOption\TimestampEncodeOption** |             |
| `$uuidEncodeOption`      | **\Cassandra\Value\EncodeOption\UuidEncodeOption**      |             |
| `$varintEncodeOption`    | **\Cassandra\Value\EncodeOption\VarintEncodeOption**    |             |

***

### default

```php
public static default(): self
```

* This method is **static**.
***
