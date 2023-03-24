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
     * Stream containing the raw result data
     */
    protected StreamReader $_stream;

    /**
     * Offset to start reading data in this stream
     */
    protected int $_offset;

    /**
     * @var array{
     *  flags: int,
     *  columns_count: int,
     *  page_state?: ?string,
     *  new_metadata_id?: string,
     *  pk_count?: int,
     *  pk_index?: int[],
     *  columns?: array<array{
     *   keyspace: string,
     *   tableName: string,
     *   name: string,
     *   type: int|array<mixed>,
     *  }>,
     * } $_metadata
     */
    protected array $_metadata;

    /**
     * Class to use for each row of data
     *
     * @var ?class-string<RowClass> $_rowClass
     */
    protected ?string $_rowClass;

    /**
     * Number of available rows in the resultset
     */
    protected int $_count;

    /**
     * Current row
     */
    protected int $_row = 0;

    /**
     * @param array{
     *  flags: int,
     *  columns_count: int,
     *  page_state?: ?string,
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

        $this->_stream = clone $stream;
        $this->_metadata = $metadata;
        $this->_rowClass = $rowClass;
        $this->_count = $this->_stream->readInt();
        $this->_offset = $this->_stream->pos();
        $this->_row = 0;
    }

    /**
     * @return ArrayObject<string, mixed>|array<string, mixed>|null
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function current(): ArrayObject|array|null {
        $data = [];

        if (isset($this->_metadata['columns'])) {
            foreach ($this->_metadata['columns'] as $column) {
                /** @psalm-suppress MixedAssignment */
                $data[$column['name']] = $this->_stream->readValue($column['type']);
            }
        }

        if ($this->_rowClass === null) {
            return $data;
        }

        /** @var ArrayObject<string, mixed> $row
         */
        $row = new $this->_rowClass($data);

        return $row;
    }

    /**
     * The current position in this result set
     */
    public function key(): int {
        return $this->_row;
    }

    /**
     * Move forward to next element
     */
    public function next(): void {
        $this->_row++;
    }

    /**
     * Reset the result set
     */
    public function rewind(): void {
        $this->_row = 0;
        $this->_stream->offset($this->_offset);
    }

    /**
     * Checks if current position is valid
     */
    public function valid(): bool {
        return (($this->_row >= 0) && ($this->_row < $this->_count));
    }
}
