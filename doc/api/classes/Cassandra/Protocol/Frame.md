# Frame

***

* Full name: `\Cassandra\Protocol\Frame`

## Methods

### getBody

```php
public getBody(): string
```

***

### getFlags

```php
public getFlags(): int
```

***

### getOpcode

```php
public getOpcode(): \Cassandra\Protocol\Opcode
```

***

### getProtocolVersion

```php
public getProtocolVersion(): \Cassandra\Protocol\ProtocolVersion
```

***

### getStream

The stream id this frame belongs to, or null for a request that has not
been assigned one yet. A response always has one, so
{@see \Cassandra\Response\Response::getStream()} narrows this to int.

```php
public getStream(): ?int
```

***

### getVersion

```php
public getVersion(): int
```

* **Warning:** this method is **deprecated**. This means that this method will likely be removed in a future version.
***
