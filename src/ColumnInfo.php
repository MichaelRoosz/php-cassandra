<?php

declare(strict_types=1);

namespace Cassandra;

use Cassandra\TypeInfo\TypeInfo;

class ColumnInfo {
    public function __construct(
        public string $keyspace,
        public string $tableName,
        public string $name,
        public TypeInfo $type,
    ) {
    }
}
