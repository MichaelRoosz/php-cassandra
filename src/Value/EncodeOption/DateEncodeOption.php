<?php

declare(strict_types=1);

namespace Cassandra\Value\EncodeOption;

enum DateEncodeOption: string {
    case AS_DATETIME_IMMUTABLE = 'DateTimeImmutable';

    /** An unsigned integer representing days with epoch centered at 2^31 (unix epoch January 1st, 1970). 
     *  2^31: 1970-1-1
     */
    case AS_INT = 'int';

    /** yyyy-mm-dd */
    case AS_STRING = 'string';
}
