<?php

declare(strict_types=1);

namespace Cassandra\Response\Result;

final class RowsMetadata {
    public function __construct(
        public readonly int $flags,

        public readonly int $columnsCount,

        public readonly ?string $pagingState,

        public readonly ?string $metadataId,

        /** @var ?ColumnInfo[] $columns */
        public readonly ?array $columns,
    ) {
    }

    public function mergeWithPreviousMetadata(RowsMetadata $previousMetadata): self {

        // keep the updated metadata
        if ($this->metadataId !== null) {
            return $this;
        }

        // if the current metadata has columns, use them
        if ($this->columns !== null) {
            $columns = $this->columns;
        } else {
            $columns = $previousMetadata->columns;
        }

        $metadataId = $previousMetadata->metadataId;

        return new self(
            flags: $this->flags,
            columnsCount: $this->columnsCount,
            pagingState: $this->pagingState,
            metadataId: $metadataId,
            columns: $columns,
        );
    }
}
