<?php

declare(strict_types=1);

namespace Cassandra;

enum Type: int {
    case ASCII = 0x0001;
    case BIGINT = 0x0002;
    case BLOB = 0x0003;
    case BOOLEAN = 0x0004;
    case COLLECTION_LIST = 0x0020;
    case COLLECTION_MAP = 0x0021;
    case COLLECTION_SET = 0x0022;
    case COUNTER = 0x0005;
    case CUSTOM = 0x0000;
    case DATE = 0x0011;
    case DECIMAL = 0x0006;
    case DOUBLE = 0x0007;
    case DURATION = 0x0015;
    case FLOAT = 0x0008;
    case INET = 0x0010;
    case INT = 0x0009;
    case SMALLINT = 0x0013;
    case TEXT = 0x000A; // deprecated in protocol v3
    case TIME = 0x0012;
    case TIMESTAMP = 0x000B;
    case TIMEUUID = 0x000F;
    case TINYINT = 0x0014;
    case TUPLE = 0x0031;
    case UDT = 0x0030;
    case UUID = 0x000C;
    case VARCHAR = 0x000D;
    case VARINT = 0x000E;
    case VECTOR = 0x0032;
}
