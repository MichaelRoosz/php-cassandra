<?php

declare(strict_types=1);

namespace Cassandra\Response\Event\Data;

final class SchemaChangeData extends EventData {
    public function __construct(
        public readonly SchemaChangeType $changeType,
        public readonly SchemaChangeTarget $target,
        public readonly string $keyspace,
        public readonly string $name,

        /** @var ?string[] $argumentTypes */
        public readonly ?array $argumentTypes = null,
    ) {
        parent::__construct();
    }
}
