<?php

declare(strict_types=1);

namespace Cassandra\Response\Result;

final class Metadata {
    public function __construct(
        public readonly int $flags,

        public readonly int $columnsCount,

        public readonly ?string $newMetadataId,

        public readonly ?string $pagingState,

        public readonly ?int $pkCount, // only present in PREPARE_RESULT metadata

        /** @var int[]|null $pkIndex */
        public readonly ?array $pkIndex, // only present in PREPARE_RESULT metadata

        /** @var ?ColumnInfo[] $columns */
        public readonly ?array $columns,
    ) {
    }

    public function mergeWithPreviousMetadata(Metadata $previousMetadata): self {

        return new self(
            flags: $this->flags,
            columnsCount: $this->columnsCount,
            newMetadataId: $this->newMetadataId ?? $previousMetadata->newMetadataId,
            pagingState: $this->pagingState,
            pkCount: $this->pkCount ?? $previousMetadata->pkCount,
            pkIndex: $this->pkIndex ?? $previousMetadata->pkIndex,
            columns: $this->columns ?? $previousMetadata->columns,
        );
    }
}
