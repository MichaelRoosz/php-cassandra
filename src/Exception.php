<?php

declare(strict_types=1);

namespace Cassandra;

use Exception as PhpException;
use Throwable;

class Exception extends PhpException {
    public const CODE_AUTH_FAILED = 1005;
    public const CODE_AUTH_INVALID_NODE_IMPLEMENTATION = 1004;
    public const CODE_AUTH_MISSING_CREDENTIALS = 1003;
    public const CODE_CANNOT_READ_RESPONSE_HEADER = 1301;
    public const CODE_COMPRESSION_NOT_SUPPORTED = 1105;

    /**
     * Request/response validation: 1100-1199
     */
    public const CODE_EXECUTE_UNEXPECTED_RESPONSE = 1100;
    public const CODE_GET_NEXT_NULL_RESPONSE = 1106;
    public const CODE_INVALID_OPCODE_TYPE = 1302;
    public const CODE_NOT_CONNECTED = 1001;
    public const CODE_OPTIONS_UNEXPECTED_RESPONSE = 1002;
    public const CODE_PREPARE_UNEXPECTED_RESPONSE = 1101;

    /**
     * Low-level frame parsing / IO: 1300-1399
     */
    public const CODE_PROTOCOL_VERSION_MISMATCH = 1300;
    public const CODE_QUERY_UNEXPECTED_RESPONSE = 1102;
    public const CODE_READY_INVALID_NODE_IMPLEMENTATION = 1006;
    public const CODE_REPREPARATION_UNEXPECTED_RESPONSE = 1206;
    public const CODE_REPREPARE_ORIGINAL_NOT_EXECUTE = 1201;
    public const CODE_REPREPARE_UNEXPECTED_RESPONSE_REEXECUTE = 1202;

    /**
     * Reprepare/unprepared handling: 1200-1299
     */
    public const CODE_REPREPARE_UNEXPECTED_RESULT_TYPE = 1200;
    public const CODE_SERVER_PROTOCOL_UNSUPPORTED = 1104;
    public const CODE_SET_KEYSPACE_UNEXPECTED_RESPONSE = 1116;
    public const CODE_STARTUP_UNEXPECTED_RESPONSE = 1007;

    // Statement result accessors
    public const CODE_STATEMENT_UNEXPECTED_PREPARED_RESULT = 1111;
    public const CODE_STATEMENT_UNEXPECTED_RESULT = 1112;
    public const CODE_STATEMENT_UNEXPECTED_ROWS_RESULT = 1113;
    public const CODE_STATEMENT_UNEXPECTED_SCHEMA_CHANGE_RESULT = 1114;
    public const CODE_STATEMENT_UNEXPECTED_SET_KEYSPACE_RESULT = 1115;
    public const CODE_SYNC_NULL_RESPONSE = 1103;

    /**
     * Node selection / connectivity: 1400-1499
     */
    public const CODE_UNABLE_TO_CONNECT_ANY_NODE = 1400;
    /**
     * Error code constants for all exceptions thrown from top-level classes in `src/` (no subfolders).
     *
     * Connection lifecycle and protocol negotiation: 1000-1099
     */
    public const CODE_UNEXPECTED_RESPONSE_BATCH_SYNC = 1000;
    public const CODE_UNKNOWN_ERROR_CODE = 1110;
    public const CODE_UNKNOWN_EVENT_TYPE = 1109;
    public const CODE_UNKNOWN_RESPONSE_TYPE = 1107;
    public const CODE_UNKNOWN_RESULT_KIND = 1108;
    public const CODE_UNPREPARED_PREV_NO_REQUEST = 1204;
    public const CODE_UNPREPARED_PREV_NOT_PREPARE_REQUEST = 1205;
    public const CODE_UNPREPARED_UNEXPECTED_PREV_RESULT_TYPE = 1203;
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
