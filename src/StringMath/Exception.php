<?php

declare(strict_types=1);

namespace Cassandra\StringMath;

use Cassandra\Exception as CassandraException;

final class Exception extends CassandraException {
    // code range 80000 to 89999
    public const CODE_INVALID_HEX_STRING = 80000;
}
