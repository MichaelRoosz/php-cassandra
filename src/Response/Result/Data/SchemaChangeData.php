<?php

declare(strict_types=1);

namespace Cassandra\Response\Result\Data;

use Cassandra\Response\Event\Data\SchemaChangeTarget;
use Cassandra\Response\Event\Data\SchemaChangeType;

final class SchemaChangeData extends ResultData {
    public function __construct(
        public readonly SchemaChangeType $changeType,
        public readonly SchemaChangeTarget $target,
        public readonly string $keyspace,
        public readonly ?string $name = null,

        /** @var ?string[] $argumentTypes */
        public readonly ?array $argumentTypes = null,
    ) {
        parent::__construct();
    }
}
