<?php

declare(strict_types=1);

namespace Cassandra\Value\EncodeOption;

enum TimeEncodeOption: string {
    /** A PHP DateTimeImmutable - precision: microseconds */
    case AS_DATETIME_IMMUTABLE = 'DateTimeImmutable';

    /** Nanoseconds since midnight - precision: nanoseconds */
    case AS_INT = 'int';

    /** hh:mm:ss.fffffffff - precision: nanoseconds */
    case AS_STRING = 'string';
}
