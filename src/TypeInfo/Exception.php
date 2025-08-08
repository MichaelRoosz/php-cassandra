<?php

declare(strict_types=1);

namespace Cassandra\TypeInfo;

use Cassandra\Exception as CassandraException;

final class Exception extends CassandraException {
    // code range 100000 to 109999
    public const COLLECTION_LIST_INVALID_TYPE = 100000;
    public const COLLECTION_LIST_MISSING_TYPE = 100001;
    public const COLLECTION_LIST_MISSING_VALUETYPE = 100002;
    public const COLLECTION_MAP_INVALID_TYPE = 100003;
    public const COLLECTION_MAP_MISSING_KEYTYPE = 100004;
    public const COLLECTION_MAP_MISSING_TYPE = 100005;
    public const COLLECTION_MAP_MISSING_VALUETYPE = 100006;
    public const COLLECTION_SET_INVALID_TYPE = 100007;
    public const COLLECTION_SET_MISSING_TYPE = 100008;
    public const COLLECTION_SET_MISSING_VALUETYPE = 100009;
    public const CUSTOM_INVALID_TYPE = 100010;
    public const CUSTOM_JAVA_CLASS_NOT_STRING = 100011;
    public const CUSTOM_MISSING_JAVA_CLASS = 100012;
    public const CUSTOM_MISSING_TYPE = 100013;
    public const SIMPLE_MISSING_TYPE = 100014;
    public const SIMPLE_NOT_SIMPLE_TYPE = 100015;
    public const TUPLE_INVALID_TYPE = 100016;
    public const TUPLE_MISSING_TYPE = 100017;
    public const TUPLE_MISSING_VALUETYPES = 100018;
    public const TUPLE_VALUETYPES_NOT_ARRAY = 100019;
    public const UDT_INVALID_TYPE = 100020;
    public const UDT_MISSING_TYPE = 100021;
    public const UDT_MISSING_VALUETYPES = 100022;
    public const UDT_VALUETYPES_NOT_ARRAY = 100023;
}
