<?php

declare(strict_types=1);

namespace Cassandra\Response\Result;

final class PrepareMetadata {
    public function __construct(
        public readonly int $flags,

        public readonly int $bindMarkersCount,

        /** @var ?ColumnInfo[] $bindMarkers */
        public readonly ?array $bindMarkers,

        public readonly ?int $pkCount,

        /** @var int[]|null $pkIndex */
        public readonly ?array $pkIndex,
    ) {
    }

    public function mergeWithPreviousMetadata(PrepareMetadata $previousMetadata): self {

        return new self(
            flags: $this->flags,
            bindMarkersCount: $this->bindMarkersCount,
            bindMarkers: $this->bindMarkers ?? $previousMetadata->bindMarkers,
            pkCount: $this->pkCount ?? $previousMetadata->pkCount,
            pkIndex: $this->pkIndex ?? $previousMetadata->pkIndex,
        );
    }
}
