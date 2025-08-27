<?php

declare(strict_types=1);

namespace Cassandra;

enum TypeName: string {
    case ASCII = 'org.apache.cassandra.db.marshal.AsciiType';
    case BOOLEAN = 'org.apache.cassandra.db.marshal.BooleanType';
    case BYTE = 'org.apache.cassandra.db.marshal.ByteType';
    case BYTES = 'org.apache.cassandra.db.marshal.BytesType';
    case COUNTER_COLUMN = 'org.apache.cassandra.db.marshal.CounterColumnType';
    case DECIMAL = 'org.apache.cassandra.db.marshal.DecimalType';
    case DOUBLE = 'org.apache.cassandra.db.marshal.DoubleType';
    case DURATION = 'org.apache.cassandra.db.marshal.DurationType';
    case FLOAT = 'org.apache.cassandra.db.marshal.FloatType';
    case FROZEN = 'org.apache.cassandra.db.marshal.FrozenType';
    case INET_ADDRESS = 'org.apache.cassandra.db.marshal.InetAddressType';
    case INT32 = 'org.apache.cassandra.db.marshal.Int32Type';
    case INTEGER = 'org.apache.cassandra.db.marshal.IntegerType';
    case LIST = 'org.apache.cassandra.db.marshal.ListType';
    case LONG = 'org.apache.cassandra.db.marshal.LongType';
    case MAP = 'org.apache.cassandra.db.marshal.MapType';
    case REVERSED = 'org.apache.cassandra.db.marshal.ReversedType';
    case SET = 'org.apache.cassandra.db.marshal.SetType';
    case SHORT = 'org.apache.cassandra.db.marshal.ShortType';
    case SIMPLE_DATE = 'org.apache.cassandra.db.marshal.SimpleDateType';
    case TIME = 'org.apache.cassandra.db.marshal.TimeType';
    case TIME_UUID = 'org.apache.cassandra.db.marshal.TimeUUIDType';
    case TIMESTAMP = 'org.apache.cassandra.db.marshal.TimestampType';
    case TUPLE = 'org.apache.cassandra.db.marshal.TupleType';
    case UDT = 'org.apache.cassandra.db.marshal.UserType';
    case UTF8 = 'org.apache.cassandra.db.marshal.UTF8Type';
    case UUID = 'org.apache.cassandra.db.marshal.UUIDType';
    case VECTOR = 'org.apache.cassandra.db.marshal.VectorType';
}
