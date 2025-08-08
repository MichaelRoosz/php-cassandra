<?php

declare(strict_types=1);

namespace Cassandra\Compression;

use Cassandra\Exception as CassandraException;

final class Exception extends CassandraException {
    public const CODE_CHECKSUM_DECODE_FAILURE = 1008;
    public const CODE_CHECKSUM_MISMATCH = 1009;
    public const CODE_DECODE_FAILURE = 1003;
    public const CODE_ILLEGAL_VALUE = 1004;
    public const CODE_INPUT_OVERFLOW = 1002;
    /**
     * Error codes for compression-related failures.
     * Keeping these granular enables downstream clients to branch on error semantics.
     */
    public const CODE_INVALID_MAGIC = 1001;
    public const CODE_INVALID_VERSION = 1007;
    public const CODE_OUTPUT_OVERFLOW = 1006;
    public const CODE_OUTPUT_UNDERFLOW = 1005;
}
