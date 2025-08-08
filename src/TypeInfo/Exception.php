<?php

declare(strict_types=1);

namespace Cassandra\TypeInfo;

use Cassandra\Exception as CassandraException;

final class Exception extends CassandraException {
    public const COLLECTION_LIST_INVALID_TYPE = 16002;

    // Collection List
    public const COLLECTION_LIST_MISSING_TYPE = 16001;
    public const COLLECTION_LIST_MISSING_VALUETYPE = 16003;
    public const COLLECTION_MAP_INVALID_TYPE = 15002;
    public const COLLECTION_MAP_MISSING_KEYTYPE = 15003;

    // Collection Map
    public const COLLECTION_MAP_MISSING_TYPE = 15001;
    public const COLLECTION_MAP_MISSING_VALUETYPE = 15004;
    public const COLLECTION_SET_INVALID_TYPE = 14002;

    // Collection Set
    public const COLLECTION_SET_MISSING_TYPE = 14001;
    public const COLLECTION_SET_MISSING_VALUETYPE = 14003;
    public const CUSTOM_INVALID_TYPE = 13002;
    public const CUSTOM_JAVA_CLASS_NOT_STRING = 13004;
    public const CUSTOM_MISSING_JAVA_CLASS = 13003;

    // Custom
    public const CUSTOM_MISSING_TYPE = 13001;

    // Simple
    public const SIMPLE_MISSING_TYPE = 12001;
    public const SIMPLE_NOT_SIMPLE_TYPE = 12002;
    public const TUPLE_INVALID_TYPE = 10002;
    // Tuple
    public const TUPLE_MISSING_TYPE = 10001;
    public const TUPLE_MISSING_VALUETYPES = 10003;
    public const TUPLE_VALUETYPES_NOT_ARRAY = 10004;
    public const UDT_INVALID_TYPE = 11002;

    // UDT
    public const UDT_MISSING_TYPE = 11001;
    public const UDT_MISSING_VALUETYPES = 11003;
    public const UDT_VALUETYPES_NOT_ARRAY = 11004;
}
