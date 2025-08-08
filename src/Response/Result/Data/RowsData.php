<?php

declare(strict_types=1);

namespace Cassandra\Response\Result\Data;

final class RowsData extends ResultData {
    public function __construct(
        /** @var list<array<array-key, mixed>> $rows */
        public readonly array $rows,
    ) {
        parent::__construct();
    }
}
