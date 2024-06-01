<?php

declare(strict_types=1);

namespace Cassandra\Response;

use ArrayObject;
use Iterator;

/**
 * @implements Iterator<ArrayObject<string, mixed>|array<string, mixed>|null>
 */
class ResultIterator implements Iterator {
    /**
     * Number of available rows in the resultset
     */
    protected int $count;

    /**
     * @var array{
     *  flags: int,
     *  columns_count: int,
     *  paging_state?: ?string,
     *  new_metadata_id?: string,
     *  pk_count?: int,
     *  pk_index?: int[],
     *  columns?: array<array{
     *   keyspace: string,
     *   tableName: string,
     *   name: string,
     *   type: int|array<mixed>,
     *  }>,
     * } $metadata
     */
    protected array $metadata;

    /**
     * Offset to start reading data in this stream
     */
    protected int $offset;

    /**
     * Current row
     */
    protected int $row = 0;

    /**
     * Class to use for each row of data
     *
     * @var ?class-string<RowClass> $rowClass
     */
    protected ?string $rowClass;

    /**
     * Stream containing the raw result data
     */
    protected StreamReader $stream;

    /**
     * @param array{
     *  flags: int,
     *  columns_count: int,
     *  paging_state?: ?string,
     *  new_metadata_id?: string,
     *  pk_count?: int,
     *  pk_index?: int[],
     *  columns?: array<array{
     *   keyspace: string,
     *   tableName: string,
     *   name: string,
     *   type: int|array<mixed>,
     *  }>,
     * } $metadata
     * @param class-string<RowClass> $rowClass
     *
     * @throws \Cassandra\Response\Exception
     */
    public function __construct(StreamReader $stream, array $metadata, ?string $rowClass = null) {
        if ($rowClass !== null && !is_subclass_of($rowClass, ArrayObject::class)) {
            throw new Exception('row class "' . $rowClass . '" is not a subclass of ArrayObject');
        }

        $this->stream = clone $stream;
        $this->metadata = $metadata;
        $this->rowClass = $rowClass;
        $this->count = $this->stream->readInt();
        $this->offset = $this->stream->pos();
        $this->row = 0;
    }

    /**
     * @return ArrayObject<string, mixed>|array<string, mixed>|null
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function current(): ArrayObject|array|null {
        $data = [];

        if (isset($this->metadata['columns'])) {
            foreach ($this->metadata['columns'] as $column) {
                /** @psalm-suppress MixedAssignment */
                $data[$column['name']] = $this->stream->readValue($column['type']);
            }
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
    public function key(): int {
        return $this->row;
    }

    /**
     * Move forward to next element
     */
    public function next(): void {
        $this->row++;
    }

    /**
     * Reset the result set
     */
    public function rewind(): void {
        $this->row = 0;
        $this->stream->offset($this->offset);
    }

    /**
     * Checks if current position is valid
     */
    public function valid(): bool {
        return (($this->row >= 0) && ($this->row < $this->count));
    }
}
