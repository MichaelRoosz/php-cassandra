<?php

declare(strict_types=1);

namespace Cassandra\Response;

use Cassandra\Exception as CassandraException;

final class Exception extends CassandraException {
    // Error frame parsing (73000-range)
    public const ERROR_INVALID_TYPE = 73000;

    // Event errors (72000-range)
    public const EVENT_INVALID_TYPE = 72000;
    public const EVENT_SCHEMA_CHANGE_INVALID_TARGET = 72011;
    public const EVENT_SCHEMA_CHANGE_INVALID_TYPE = 72010;
    public const EVENT_SCHEMA_CHANGE_UNEXPECTED_TARGET_VALUE = 72012;
    public const EVENT_STATUS_CHANGE_INVALID_TYPE = 72020;
    public const EVENT_TOPOLOGY_CHANGE_INVALID_TYPE = 72030;

    // Prepared/SetKeyspace/Void result kind errors (71300-range)
    public const PREPARED_UNEXPECTED_KIND = 71300;

    // ProgressiveStreamReader errors (70100-range)
    public const PSR_GET_DATA_NOT_SUPPORTED = 70100;
    public const PSR_INCOMPLETE_RESPONSE = 70101;
    public const READ_FAILURE_INVALID_CONSISTENCY = 73210;
    public const READ_TIMEOUT_INVALID_CONSISTENCY = 73200;
    public const RES_INVALID_KIND_VALUE = 71002;
    public const RES_METADATA_NOT_AVAILABLE = 71001;

    // Result base errors (71000-range)
    public const RES_PREPARED_CONTEXT_NOT_FOUND = 71000;
    public const ROWS_INVALID_KEY_INDEX = 71103;
    public const ROWS_INVALID_KEY_TYPE = 71102;

    // RowsResult errors (71100-range)
    public const ROWS_INVALID_ROWCLASS = 71100;
    public const ROWS_NO_COLUMN_METADATA = 71101;
    public const ROWS_ROWCLASS_NOT_SUBCLASS = 71104;
    public const SCHEMA_CHANGE_INVALID_TARGET = 71202;
    public const SCHEMA_CHANGE_INVALID_TYPE = 71201;

    // SchemaChangeResult errors (71200-range)
    public const SCHEMA_CHANGE_UNEXPECTED_KIND = 71200;
    public const SCHEMA_CHANGE_UNEXPECTED_TARGET_VALUE = 71203;
    public const SET_KEYSPACE_UNEXPECTED_KIND = 71301;
    public const SR_INET_PARSE_FAIL = 70005;
    public const SR_INVALID_INET_LENGTH = 70004;
    public const SR_INVALID_MAP_KEY_TYPE = 70007;
    public const SR_INVALID_TEXT_LIST_ITEM = 70011;
    public const SR_INVALID_TYPE_DISCRIMINATOR = 70012;
    public const SR_READ_BEYOND_AVAILABLE = 70015;
    /**
     * Unique, internal client-side error codes for Response-layer exceptions.
     * These DO NOT overlap with server error codes. Server-provided error
     * codes are surfaced directly via Error::getException() and remain
     * unchanged.
     */
    // StreamReader errors (70000-range)
    public const SR_READ_BYTES_LENGTH_UNPACK_FAIL = 70001;
    public const SR_UNPACK_DOUBLE_FAIL = 70002;
    public const SR_UNPACK_FLOAT_FAIL = 70003;
    public const SR_UNPACK_INT_FAIL = 70006;
    public const SR_UNPACK_LONGSTRING_LENGTH_FAIL = 70008;
    public const SR_UNPACK_SHORT_FAIL = 70009;
    public const SR_UNPACK_STRING_LENGTH_FAIL = 70010;
    public const SR_UNPACK_UUID_FAIL = 70013;
    public const SR_UNPACK_VALUE_LENGTH_FAIL = 70014;
    public const UNAVAILABLE_INVALID_CONSISTENCY = 73300;
    public const VOID_UNEXPECTED_KIND = 71302;
    public const WRITE_FAILURE_INVALID_CONSISTENCY = 73110;
    public const WRITE_FAILURE_INVALID_WRITE_TYPE = 73111;

    // Error contexts (write/read/unavailable) (73100+)
    public const WRITE_TIMEOUT_INVALID_CONSISTENCY = 73100;
    public const WRITE_TIMEOUT_INVALID_WRITE_TYPE = 73101;
}
