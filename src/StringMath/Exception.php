<?php

declare(strict_types=1);

namespace Cassandra\StringMath;

use Cassandra\Exception as CassandraException;

final class Exception extends CassandraException {
    /**
     * Unique error code when a non-hexadecimal character is found during hex-to-decimal conversion.
     */
    public const CODE_INVALID_HEX_STRING = 1001;
}
