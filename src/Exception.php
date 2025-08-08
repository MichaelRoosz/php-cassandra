<?php

declare(strict_types= 1);

namespace Cassandra;

use Exception as PhpException;
use Throwable;

class Exception extends PhpException {
    // code range 10000 to 19999
    public const CODE_AUTH_FAILED = 10000;
    public const CODE_AUTH_INVALID_NODE_IMPLEMENTATION = 10001;
    public const CODE_AUTH_MISSING_CREDENTIALS = 10002;
    public const CODE_CANNOT_READ_RESPONSE_HEADER = 10003;
    public const CODE_COMPRESSION_NOT_SUPPORTED = 10004;
    public const CODE_EXECUTE_UNEXPECTED_RESPONSE = 10005;
    public const CODE_GET_NEXT_NULL_RESPONSE = 10006;
    public const CODE_INVALID_OPCODE_TYPE = 10007;
    public const CODE_NOT_CONNECTED = 10008;
    public const CODE_OPTIONS_UNEXPECTED_RESPONSE = 10009;
    public const CODE_PREPARE_UNEXPECTED_RESPONSE = 10010;
    public const CODE_PROTOCOL_VERSION_MISMATCH = 10011;
    public const CODE_QUERY_UNEXPECTED_RESPONSE = 10012;
    public const CODE_READY_INVALID_NODE_IMPLEMENTATION = 10013;
    public const CODE_REPREPARATION_UNEXPECTED_RESPONSE = 10014;
    public const CODE_REPREPARE_ORIGINAL_NOT_EXECUTE = 10015;
    public const CODE_REPREPARE_UNEXPECTED_RESPONSE_REEXECUTE = 10016;
    public const CODE_REPREPARE_UNEXPECTED_RESULT_TYPE = 10017;
    public const CODE_SERVER_PROTOCOL_UNSUPPORTED = 10018;
    public const CODE_SET_KEYSPACE_UNEXPECTED_RESPONSE = 10019;
    public const CODE_STARTUP_UNEXPECTED_RESPONSE = 10020;
    public const CODE_STATEMENT_UNEXPECTED_PREPARED_RESULT = 10021;
    public const CODE_STATEMENT_UNEXPECTED_RESULT = 10022;
    public const CODE_STATEMENT_UNEXPECTED_ROWS_RESULT = 10023;
    public const CODE_STATEMENT_UNEXPECTED_SCHEMA_CHANGE_RESULT = 10024;
    public const CODE_STATEMENT_UNEXPECTED_SET_KEYSPACE_RESULT = 10025;
    public const CODE_SYNC_NULL_RESPONSE = 10026;
    public const CODE_UNABLE_TO_CONNECT_ANY_NODE = 10027;
    public const CODE_UNEXPECTED_RESPONSE_BATCH_SYNC = 10028;
    public const CODE_UNKNOWN_ERROR_CODE = 10029;
    public const CODE_UNKNOWN_EVENT_TYPE = 10030;
    public const CODE_UNKNOWN_RESPONSE_TYPE = 10031;
    public const CODE_UNKNOWN_RESULT_KIND = 10032;
    public const CODE_UNPREPARED_PREV_NO_REQUEST = 10033;
    public const CODE_UNPREPARED_PREV_NOT_PREPARE_REQUEST = 10034;
    public const CODE_UNPREPARED_UNEXPECTED_PREV_RESULT_TYPE = 10035;

    /**
     * @var array<mixed> $context
     */
    protected array $context;

    /**
     * @param array<mixed> $context
     */
    public function __construct(string $message = '', int $code = 0, array $context = [], ?Throwable $previous = null) {
        parent::__construct($message, $code, $previous);

        $this->context = $context;
    }

    /**
     * @return array<mixed> $context
     */
    public function context(): array {
        return $this->context;
    }

    /**
     * @return array<mixed> $context
     */
    public function getContext(): array {
        return $this->context;
    }
}
