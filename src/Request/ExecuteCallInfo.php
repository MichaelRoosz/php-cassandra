<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Metadata;

final class ExecuteCallInfo {
    public function __construct(
        public readonly string $id,
        public readonly Metadata $queryMetadata,
        public readonly ?string $resultMetadataId = null,
    ) {
    }
}
