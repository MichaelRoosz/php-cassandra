# DurationEncodeOption

***

* Full name: `\Cassandra\Value\EncodeOption\DurationEncodeOption`
* This enum is backed by `string`

## Cases

| Case                     | Value                  | Description                                                                               |
|--------------------------|------------------------|-------------------------------------------------------------------------------------------|
| `AS_DATEINTERVAL`        | `'DateInterval'`       | A PHP DateInterval - precision: microseconds                                              |
| `AS_DATEINTERVAL_STRING` | `'DateIntervalString'` | A PHP DateInterval string - precision: seconds                                            |
| `AS_NATIVE_VALUE`        | `'Native'`             | An array (with keys 'months'. 'days', 'nanoseconds') - precision: nanoseconds             |
| `AS_STRING`              | `'string'`             | Example: "1y2mo3d4h5m6s7ms8us9ns", starts with a "-" if negative - precision: nanoseconds |
