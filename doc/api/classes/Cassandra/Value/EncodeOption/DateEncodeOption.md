# DateEncodeOption

***

* Full name: `\Cassandra\Value\EncodeOption\DateEncodeOption`
* This enum is backed by `string`

## Cases

| Case                    | Value                 | Description                                                                                       |
|-------------------------|-----------------------|---------------------------------------------------------------------------------------------------|
| `AS_DATETIME_IMMUTABLE` | `'DateTimeImmutable'` |                                                                                                   |
| `AS_INT`                | `'int'`               | An unsigned integer representing days with epoch centered at 2^31 (unix epoch January 1st, 1970). |
| `AS_STRING`             | `'string'`            | yyyy-mm-dd                                                                                        |
