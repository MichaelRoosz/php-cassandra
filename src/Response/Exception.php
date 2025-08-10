<?php

declare(strict_types=1);

namespace Cassandra\Response;

use Cassandra\Exception as CassandraException;

final class Exception extends CassandraException {
    // code range 70000 to 79999
    public const ERROR_INVALID_TYPE = 70000;
    public const EVENT_INVALID_TYPE = 70001;
    public const EVENT_SCHEMA_CHANGE_INVALID_TARGET = 70002;
    public const EVENT_SCHEMA_CHANGE_INVALID_TYPE = 70003;
    public const EVENT_SCHEMA_CHANGE_UNEXPECTED_TARGET_VALUE = 70004;
    public const EVENT_STATUS_CHANGE_INVALID_TYPE = 70005;
    public const EVENT_TOPOLOGY_CHANGE_INVALID_TYPE = 70006;
    public const PREPARED_UNEXPECTED_KIND = 70007;
    public const PSR_GET_DATA_NOT_SUPPORTED = 70008;
    public const PSR_SOURCE_NOT_SET = 70009;
    public const READ_FAILURE_INVALID_CONSISTENCY = 70010;
    public const READ_TIMEOUT_INVALID_CONSISTENCY = 70011;
    public const RES_INVALID_KIND_VALUE = 70012;
    public const RES_METADATA_NOT_AVAILABLE = 70013;
    public const RES_NOT_PREPARED_RESULT = 70046;
    public const RES_NOT_ROWS_RESULT = 70047;
    public const RES_NOT_SCHEMA_CHANGE_RESULT = 70048;
    public const RES_NOT_SET_KEYSPACE_RESULT = 70049;
    public const RES_NOT_VOID_RESULT = 70050;
    public const RES_PREPARED_CONTEXT_NOT_FOUND = 70014;
    public const ROWS_INVALID_KEY_INDEX = 70015;
    public const ROWS_INVALID_KEY_TYPE = 70016;
    public const ROWS_INVALID_ROWCLASS = 70017;
    public const ROWS_NO_COLUMN_METADATA = 70018;
    public const ROWS_ROWCLASS_NOT_SUBCLASS = 70019;
    public const SCHEMA_CHANGE_INVALID_TARGET = 70020;
    public const SCHEMA_CHANGE_INVALID_TYPE = 70021;
    public const SCHEMA_CHANGE_UNEXPECTED_KIND = 70022;
    public const SCHEMA_CHANGE_UNEXPECTED_TARGET_VALUE = 70023;
    public const SET_KEYSPACE_UNEXPECTED_KIND = 70024;
    public const SR_INET_PARSE_FAIL = 70025;
    public const SR_INVALID_INET_LENGTH = 70026;
    public const SR_INVALID_MAP_KEY_TYPE = 70027;
    public const SR_INVALID_TEXT_LIST_ITEM = 70028;
    public const SR_INVALID_TYPE_DISCRIMINATOR = 70029;
    public const SR_READ_BEYOND_AVAILABLE = 70030;
    public const SR_READ_BYTES_LENGTH_UNPACK_FAIL = 70031;
    public const SR_UNPACK_DOUBLE_FAIL = 70032;
    public const SR_UNPACK_FLOAT_FAIL = 70033;
    public const SR_UNPACK_INT_FAIL = 70034;
    public const SR_UNPACK_LONGSTRING_LENGTH_FAIL = 70035;
    public const SR_UNPACK_SHORT_FAIL = 70036;
    public const SR_UNPACK_STRING_LENGTH_FAIL = 70037;
    public const SR_UNPACK_UUID_FAIL = 70038;
    public const SR_UNPACK_VALUE_LENGTH_FAIL = 70039;
    public const UNAVAILABLE_INVALID_CONSISTENCY = 70040;
    public const VOID_UNEXPECTED_KIND = 70041;
    public const WRITE_FAILURE_INVALID_CONSISTENCY = 70042;
    public const WRITE_FAILURE_INVALID_WRITE_TYPE = 70043;
    public const WRITE_TIMEOUT_INVALID_CONSISTENCY = 70044;
    public const WRITE_TIMEOUT_INVALID_WRITE_TYPE = 70045;
}
