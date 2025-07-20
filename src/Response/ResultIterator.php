<?php

declare(strict_types=1);

namespace Cassandra\Response;

use ArrayObject;
use Iterator;
use Cassandra\Metadata;

/**
 * @implements Iterator<ArrayObject<string, mixed>|array<string, mixed>|null>
 */
final class ResultIterator implements Iterator {
    protected int $currentRow = 0;

    /**
     * @param class-string<\Cassandra\Response\RowClass>|null $rowClass
     *
     * @throws \Cassandra\Response\Exception
     */
    public function __construct(
        protected StreamReader $stream,
        protected Metadata $metadata,
        protected int $rowCount,
        protected int $dataOffset,
        protected ?string $rowClass = null,
    ) {

        if ($rowClass !== null && !is_subclass_of($rowClass, ArrayObject::class)) {
            throw new Exception('row class "' . $rowClass . '" is not a subclass of ArrayObject');
        }

        $this->currentRow = 0;
    }

    /**
     * @return ArrayObject<string, mixed>|array<string, mixed>
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public function current(): ArrayObject|array {
        $data = [];

        if ($this->metadata->columns === null) {
            throw new Exception('Column metadata is not available');
        }

        foreach ($this->metadata->columns as $column) {
            /** @psalm-suppress MixedAssignment */
            $data[$column->name] = $this->stream->readValue($column->type);
        }

        if ($this->rowClass === null) {
            return $data;
        }

        /** @var ArrayObject<string, mixed> $row
         */
        $row = new $this->rowClass($data);

        return $row;
    }

    /**
     * The current position in this result set
     */
    #[\Override]
    public function key(): int {
        return $this->currentRow;
    }

    /**
     * Move forward to next element
     */
    #[\Override]
    public function next(): void {
        $this->currentRow++;
    }

    /**
     * Reset the result set
     */
    #[\Override]
    public function rewind(): void {
        $this->currentRow = 0;
        $this->stream->offset($this->dataOffset);
    }

    /**
     * Checks if current position is valid
     */
    #[\Override]
    public function valid(): bool {
        return (($this->currentRow >= 0) && ($this->currentRow < $this->rowCount));
    }
}
