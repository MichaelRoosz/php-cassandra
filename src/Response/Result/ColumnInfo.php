<?php

declare(strict_types=1);

namespace Cassandra\Response\Result;

use Cassandra\TypeInfo\TypeInfo;

final class ColumnInfo {
    public function __construct(
        public readonly string $keyspace,
        public readonly string $tableName,
        public readonly string $name,
        public readonly TypeInfo $type,
    ) {
    }
}
