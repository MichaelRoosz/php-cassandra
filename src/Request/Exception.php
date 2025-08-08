<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Exception as CassandraException;

final class Exception extends CassandraException {
    // code range 60000 to 69999
    public const EXECUTE_INVALID_PREVIOUS_RESULT = 60000;
    public const EXECUTE_MISSING_RESULT_METADATA_ID = 60001;
    public const EXECUTE_PREPARED_STATEMENT_NOT_FOUND = 60002;
    public const UNSUPPORTED_OPTION_KEYSPACE = 60003;
    public const UNSUPPORTED_OPTION_NOW_IN_SECONDS = 60004;
    public const VALUES_NAMES_FOR_VALUES_EXPECTS_ASSOCIATIVE = 60005;
    public const VALUES_NAMES_FOR_VALUES_EXPECTS_SEQUENTIAL = 60006;
    public const VALUES_UNSUPPORTED_VALUE_TYPE = 60007;
}
