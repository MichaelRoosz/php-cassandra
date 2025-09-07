<?php

declare(strict_types=1);

namespace Cassandra\Value\EncodeOption;

enum DurationEncodeOption: string {
    /** A PHP DateInterval - precision: microseconds */
    case AS_DATEINTERVAL = 'DateInterval';

    /** A PHP DateInterval string - precision: seconds */
    case AS_DATEINTERVAL_STRING = 'DateIntervalString';

    /** An array (with keys 'months'. 'days', 'nanoseconds') - precision: nanoseconds */
    case AS_NATIVE_VALUE = 'Native';

    /** Example: "1y2mo3d4h5m6s7ms8us9ns", starts with a "-" if negative - precision: nanoseconds */
    case AS_STRING = 'string';
}
