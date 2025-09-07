<?php

declare(strict_types=1);

namespace Cassandra\Value\EncodeOption;

enum TimestampEncodeOption: string {
    /** A PHP DateTimeImmutable - precision: milliseconds */
    case AS_DATETIME_IMMUTABLE = 'DateTimeImmutable';

    /** An 8 byte two's complement integer representing a millisecond-precision offset from the unix epoch (00:00:00, January 1st, 1970). */
    case AS_INT = 'int';

    /** Y-m-d H:i:s.vO - precision: milliseconds */
    case AS_STRING = 'string';
}
