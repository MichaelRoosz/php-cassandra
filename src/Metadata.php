<?php

declare(strict_types=1);

namespace Cassandra;

class Metadata {
    public function __construct(
        public int $flags,
        public int $columnsCount,

        public ?string $newMetadataId,
        public ?string $pagingState,

        public ?int $pkCount,
        /** @var int[]|null $pkIndex */
        public ?array $pkIndex,

        /** @var ColumnInfo[] $columns */
        public array $columns,
    ) {
    }

    public function mergeWithPreviousMetadata(Metadata $previousMetadata): void {

        $this->newMetadataId = $this->newMetadataId ?? $previousMetadata->newMetadataId;
        $this->pagingState = $this->pagingState ?? $previousMetadata->pagingState;

        $this->pkCount = $this->pkCount ?? $previousMetadata->pkCount;
        $this->pkIndex = $this->pkIndex ?? $previousMetadata->pkIndex;

        $this->columns = $this->columns ?? $previousMetadata->columns;
    }
}
