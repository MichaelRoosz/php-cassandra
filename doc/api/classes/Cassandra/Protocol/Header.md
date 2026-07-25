# Header

***

* Full name: `\Cassandra\Protocol\Header`
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Properties

### version

```php
public \Cassandra\Protocol\ProtocolVersion $version
```

***

### flags

```php
public int $flags
```

***

### stream

```php
public int $stream
```

***

### opcode

```php
public \Cassandra\Protocol\Opcode $opcode
```

***

### length

```php
public int $length
```

***

## Methods

### __construct

```php
public __construct(\Cassandra\Protocol\ProtocolVersion $version, int $flags, int $stream, \Cassandra\Protocol\Opcode $opcode, int $length): mixed
```

**Parameters:**

| Parameter  | Type                                    | Description |
|------------|-----------------------------------------|-------------|
| `$version` | **\Cassandra\Protocol\ProtocolVersion** |             |
| `$flags`   | **int**                                 |             |
| `$stream`  | **int**                                 |             |
| `$opcode`  | **\Cassandra\Protocol\Opcode**          |             |
| `$length`  | **int**                                 |             |

***
