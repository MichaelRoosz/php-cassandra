<?php

declare(strict_types=1);

namespace Cassandra\Response\Result\Data;

use Cassandra\Response\Result\Metadata;

final class PreparedData extends ResultData {
    public function __construct(
        public readonly string $id,
        public readonly Metadata $metadata,
        public readonly Metadata $resultMetadata,
        public readonly ?string $resultMetadataId = null,
    ) {
        parent::__construct();
    }
}
