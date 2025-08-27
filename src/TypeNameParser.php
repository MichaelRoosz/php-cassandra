<?php

declare(strict_types=1);

namespace Cassandra;

use Cassandra\TypeInfo\TypeInfo;

// org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.UserType(foo,696e617070726f7072696174655f666565646261636a,617574686f725f66696e6765727072696e75:org.apache.cassandra.db.marshal.UTF8Type,7375626d697373696f6e5f7473:org.apache.cassandra.db.marshal.TimestampType,726561736f6e5f74657874:org.apache.cassandra.db.marshal.UTF8Type)))
// org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UserType(foo,696e617070726f7072696174655f666565646261636a,617574686f725f66696e6765727072696e75:org.apache.cassandra.db.marshal.UTF8Type,7375626d697373696f6e5f7473:org.apache.cassandra.db.marshal.TimestampType,726561736f6e5f74657874:org.apache.cassandra.db.marshal.UTF8Type))

final class TypeNameParser {
    public function parse(string $typeName): TypeInfo {
        // todo: implement
    }

    protected function getComplexTypeMap(): array {
        return [
            TypeName::FROZEN->value => [$this, 'parseFrozenType'],
        ];
    }

    protected function getSimpleTypeMap(): array {
        return [
            TypeName::ASCII->value => Type::ASCII,
        ];
    }

    protected function parseFrozenType(string $typeName): TypeInfo {
        // todo: implement
    }

    protected function parseListType(string $typeName): TypeInfo {
        // todo: implement
    }

    protected function parseMapType(string $typeName): TypeInfo {
        // todo: implement
    }

    protected function parseReversedType(string $typeName): TypeInfo {
        // todo: implement
    }

    protected function parseSetType(string $typeName): TypeInfo {
        // todo: implement
    }

    protected function parseTupleType(string $typeName): TypeInfo {
        // todo: implement
    }

    protected function parseUDTType(string $typeName): TypeInfo {
        // todo: implement
    }

    protected function parseUserType(string $typeName): TypeInfo {
        // todo: implement
    }

    protected function parseVectorType(string $typeName): TypeInfo {
        // todo: implement

    }
}
