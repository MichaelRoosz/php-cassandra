<?php

declare(strict_types=1);

namespace Cassandra\Response\Result\Data;

final class SetKeyspaceData extends ResultData {
    public function __construct(
        public readonly string $keyspace,
    ) {
        parent::__construct();
    }
}
