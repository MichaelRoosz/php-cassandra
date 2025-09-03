<?php

declare(strict_types=1);

namespace Cassandra\Response\Result\Data;

use Cassandra\Response\Result\PrepareMetadata;
use Cassandra\Response\Result\RowsMetadata;

final class PreparedData extends ResultData {
    public function __construct(
        public readonly string $id,
        public readonly PrepareMetadata $prepareMetadata,
        public readonly RowsMetadata $rowsMetadata,
        public readonly ?string $rowsMetadataId = null,
    ) {
        parent::__construct();
    }
}
