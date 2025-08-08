<?php

declare(strict_types=1);

namespace Cassandra\Compression;

use Cassandra\Exception as CassandraException;

final class Exception extends CassandraException {
    // code range 20000 to 29999
    public const CODE_CHECKSUM_DECODE_FAILURE = 20000;
    public const CODE_CHECKSUM_MISMATCH = 20001;
    public const CODE_DECODE_FAILURE = 20002;
    public const CODE_ILLEGAL_VALUE = 20003;
    public const CODE_INPUT_OVERFLOW = 20004;
    public const CODE_INVALID_MAGIC = 20005;
    public const CODE_INVALID_VERSION = 20006;
    public const CODE_OUTPUT_OVERFLOW = 20007;
    public const CODE_OUTPUT_UNDERFLOW = 20008;
}
