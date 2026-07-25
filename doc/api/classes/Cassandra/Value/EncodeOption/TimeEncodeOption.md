# TimeEncodeOption

***

* Full name: `\Cassandra\Value\EncodeOption\TimeEncodeOption`
* This enum is backed by `string`

## Cases

| Case                    | Value                 | Description                                         |
|-------------------------|-----------------------|-----------------------------------------------------|
| `AS_DATETIME_IMMUTABLE` | `'DateTimeImmutable'` | A PHP DateTimeImmutable - precision: microseconds   |
| `AS_INT`                | `'int'`               | Nanoseconds since midnight - precision: nanoseconds |
| `AS_STRING`             | `'string'`            | hh:mm:ss.fffffffff - precision: nanoseconds         |
