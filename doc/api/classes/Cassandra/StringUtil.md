# StringUtil

ASCII character-class checks used for validating numeric and hex strings.

These intentionally avoid ext-ctype so the library needs no PHP extension.
strspn() stays roughly flat as the subject grows, whereas ctype_digit()/
ctype_xdigit() cost scales per character; including the call overhead of these
wrappers the two break even at roughly 40 characters, so strspn() is the better
fit for the typically long strings checked here (varint/decimal digit strings,
hex-encoded type names). It is also not locale-sensitive.

Note that these return false for the empty string, matching ctype_digit('') /
ctype_xdigit('') — strspn() alone would report an empty string as valid.

***

* Full name: `\Cassandra\StringUtil`
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Methods

### isDigits

Returns true if $value is a non-empty string of ASCII decimal digits only.

```php
public static isDigits(string $value): bool
```

* This method is **static**.
**Parameters:**

| Parameter | Type       | Description |
|-----------|------------|-------------|
| `$value`  | **string** |             |

***

### isHexDigits

Returns true if $value is a non-empty string of ASCII hex digits only.

```php
public static isHexDigits(string $value): bool
```

* This method is **static**.
**Parameters:**

| Parameter | Type       | Description |
|-----------|------------|-------------|
| `$value`  | **string** |             |

***
