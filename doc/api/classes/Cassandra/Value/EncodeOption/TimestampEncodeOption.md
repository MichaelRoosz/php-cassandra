# TimestampEncodeOption

***

* Full name: `\Cassandra\Value\EncodeOption\TimestampEncodeOption`
* This enum is backed by `string`

## Cases

| Case                    | Value                 | Description                                                                                                                       |
|-------------------------|-----------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| `AS_DATETIME_IMMUTABLE` | `'DateTimeImmutable'` | A PHP DateTimeImmutable - precision: milliseconds                                                                                 |
| `AS_INT`                | `'int'`               | An 8 byte two's complement integer representing a millisecond-precision offset from the unix epoch (00:00:00, January 1st, 1970). |
| `AS_STRING`             | `'string'`            | Y-m-d H:i:s.vO - precision: milliseconds                                                                                          |
