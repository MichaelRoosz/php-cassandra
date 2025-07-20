<?php

declare(strict_types=1);

namespace Cassandra\Response\Result\Data;

final class SchemaChangeData extends ResultData {
    public function __construct(
        public readonly string $changeType,
        public readonly string $target,
        public readonly string $keyspace,
        public readonly ?string $name = null,

        /** @var ?string[] $argumentTypes */
        public readonly ?array $argumentTypes = null,
    ) {
        parent::__construct();
    }
}
