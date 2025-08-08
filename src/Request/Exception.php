<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Exception as CassandraException;

final class Exception extends CassandraException {
    public const EXECUTE_INVALID_PREVIOUS_RESULT = 1101;
    public const EXECUTE_MISSING_RESULT_METADATA_ID = 1103;
    public const EXECUTE_PREPARED_STATEMENT_NOT_FOUND = 1102;
    /**
     * Error codes for exceptions thrown within the Request layer.
     * Codes are stable and suitable for programmatic handling.
     */
    public const UNSUPPORTED_OPTION_KEYSPACE = 1001;
    public const UNSUPPORTED_OPTION_NOW_IN_SECONDS = 1002;
    public const VALUES_NAMES_FOR_VALUES_EXPECTS_ASSOCIATIVE = 1202;
    public const VALUES_NAMES_FOR_VALUES_EXPECTS_SEQUENTIAL = 1203;

    public const VALUES_UNSUPPORTED_VALUE_TYPE = 1201;
}
