# ProtocolVersion

***

* Full name: `\Cassandra\Protocol\ProtocolVersion`
* This enum is backed by `int`

## Cases

| Case | Value | Description |
|------|-------|-------------|
| `V3` | `3`   |             |
| `V4` | `4`   |             |
| `V5` | `5`   |             |

## Methods

### asIncomingVersion

```php
public asIncomingVersion(): int
```

***

### fromOptionFormat

```php
public static fromOptionFormat(string $versionInOptionFormat): ?self
```

* This method is **static**.
**Parameters:**

| Parameter                | Type       | Description |
|--------------------------|------------|-------------|
| `$versionInOptionFormat` | **string** |             |

***

### getHighestSupportedVersion

```php
public static getHighestSupportedVersion(\Cassandra\Protocol\ProtocolVersion[] $availableVersions, \Cassandra\Protocol\ProtocolVersion[] $allowedVersions): ?\Cassandra\Protocol\ProtocolVersion
```

* This method is **static**.
**Parameters:**

| Parameter            | Type                                      | Description |
|----------------------|-------------------------------------------|-------------|
| `$availableVersions` | **\Cassandra\Protocol\ProtocolVersion[]** |             |
| `$allowedVersions`   | **\Cassandra\Protocol\ProtocolVersion[]** |             |

***

### inOptionFormat

```php
public inOptionFormat(): string
```

***

### supports

```php
public supports(\Cassandra\Protocol\ProtocolVersion $other): bool
```

**Parameters:**

| Parameter | Type                                    | Description |
|-----------|-----------------------------------------|-------------|
| `$other`  | **\Cassandra\Protocol\ProtocolVersion** |             |

***
