# TypeNameParser

Parser for the Java class name strings Cassandra uses to describe column types
(org.apache.cassandra.db.marshal.*).

The grammar is ASCII-only: Cassandra's own TypeParser.isIdentifierChar() accepts
exactly [0-9a-zA-Z-+._&], plus "(", ")", ",", ":" as structure and space/tab/newline
as separators. Non-ASCII UDT type and field names are hex-encoded precisely so that
they can round-trip through that grammar. This parser is therefore byte-oriented and
needs no mbstring; the only place arbitrary bytes can appear is after hex-decoding a
UDT name, which decodeUdtName() validates as UTF-8.

***

* Full name: `\Cassandra\TypeNameParser`
* This class is marked as **final** and can't be subclassed
* This class is a **Final class**

## Methods

### parse

```php
public parse(string $typeString, bool $isFrozen = false): \Cassandra\TypeInfo\TypeInfo
```

**Parameters:**

| Parameter     | Type       | Description |
|---------------|------------|-------------|
| `$typeString` | **string** |             |
| `$isFrozen`   | **bool**   |             |

**Throws:**

- [`TypeNameParserException`](./Exception/TypeNameParserException.md)

***
