# UuidEncodeOption

***

* Full name: `\Cassandra\Value\EncodeOption\UuidEncodeOption`
* This enum is backed by `string`

## Cases

| Case        | Value      | Description                                                                    |
|-------------|------------|--------------------------------------------------------------------------------|
| `AS_BINARY` | `'binary'` | The raw 16-byte big-endian binary form (no formatting; fastest to decode).     |
| `AS_STRING` | `'string'` | The canonical 36-character string form (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx). |
