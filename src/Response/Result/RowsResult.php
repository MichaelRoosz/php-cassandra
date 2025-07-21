<?php

declare(strict_types=1);

namespace Cassandra\Response\Result;

use ArrayObject;
use Cassandra\Metadata;
use Cassandra\Protocol\Header;
use Cassandra\Response\Exception;
use Cassandra\Response\Result;
use Cassandra\Response\Result\Data\ResultData;
use Cassandra\Response\Result\Data\RowsData;
use Cassandra\Response\ResultIterator;
use Cassandra\Response\StreamReader;

final class RowsResult extends Result {
    protected Metadata $metadata;

    /**
     * @var class-string<\Cassandra\Response\RowClass> $rowClass
     */
    protected ?string $rowClass = null;

    protected int $rowCount = 0;

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    final public function __construct(Header $header, StreamReader $stream) {

        parent::__construct(
            header: $header,
            stream: $stream,
        );

        $this->metadata = $this->readRowsMetadata();
        $this->rowCount = $this->stream->readInt();

        $this->dataOffset = $this->stream->pos();
    }

    /**
     * @param class-string<\Cassandra\Response\RowClass> $rowClass
     * @return array<\ArrayObject<string, mixed>|array<string, mixed>>
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function fetchAll(?string $rowClass = null): array {

        $this->stream->offset($this->dataOffset);

        $rows = [];

        if ($rowClass === null) {
            $rowClass = $this->rowClass;
        }

        if ($rowClass !== null && !is_subclass_of($rowClass, ArrayObject::class)) {
            throw new Exception('row class "' . $rowClass . '" is not a subclass of ArrayObject');
        }

        if ($this->metadata->columns === null) {
            throw new Exception('Column metadata is not available');
        }

        for ($i = 0; $i < $this->rowCount; ++$i) {
            $data = [];

            foreach ($this->metadata->columns as $column) {
                /** @psalm-suppress MixedAssignment */
                $data[$column->name] = $this->stream->readValue($column->type);
            }

            if ($rowClass === null) {
                $rows[$i] = $data;
            } else {
                $rows[$i] = new $rowClass($data);
            }
        }

        return $rows;
    }

    /**
     * @return array<mixed>
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function fetchCol(int $index = 0): array {

        $this->stream->offset($this->dataOffset);

        $array = [];

        if ($this->metadata->columns === null) {
            throw new Exception('Column metadata is not available');
        }

        for ($i = 0; $i < $this->rowCount; ++$i) {

            /** @psalm-suppress MixedAssignment */
            foreach ($this->metadata->columns as $j => $column) {
                $value = $this->stream->readValue($column->type);

                if ($j === $index) {
                    $array[$i] = $value;
                }
            }
        }

        return $array;
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function fetchOne(): mixed {

        $this->stream->offset($this->dataOffset);

        if ($this->rowCount === 0) {
            return null;
        }

        if ($this->metadata->columns === null) {
            throw new Exception('Column metadata is not available');
        }

        foreach ($this->metadata->columns as $column) {
            return $this->stream->readValue($column->type);
        }

        return null;
    }

    /**
     * @return array<mixed>
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function fetchPairs(): array {

        $this->stream->offset($this->dataOffset);

        $map = [];

        if ($this->metadata->columns === null) {
            throw new Exception('Column metadata is not available');
        }

        for ($i = 0; $i < $this->rowCount; ++$i) {
            $key = null;

            /** @psalm-suppress MixedAssignment */
            foreach ($this->metadata->columns as $j => $column) {
                $value = $this->stream->readValue($column->type);

                if ($j === 0) {
                    $key = $value;
                    if (!is_int($key) && !is_string($key)) {
                        throw new Exception('Invalid key type');
                    }
                } elseif ($j === 1 && $key !== null) {
                    $map[$key] = $value;
                }
            }
        }

        return $map;
    }

    /**
     * @param class-string<\Cassandra\Response\RowClass> $rowClass
     * @return \ArrayObject<string, mixed>|array<string, mixed>
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function fetchRow(?string $rowClass = null): ArrayObject|array {

        $this->stream->offset($this->dataOffset);

        if ($rowClass === null) {
            $rowClass = $this->rowClass;
        }

        if ($rowClass !== null && !is_subclass_of($rowClass, ArrayObject::class)) {
            throw new Exception('row class "' . $rowClass . '" is not a subclass of ArrayObject');
        }

        $data = [];

        if ($this->metadata->columns === null) {
            throw new Exception('Column metadata is not available');
        }

        for ($i = 0; $i < $this->rowCount && $i < 1; ++$i) {
            foreach ($this->metadata->columns as $column) {
                /** @psalm-suppress MixedAssignment */
                $data[$column->name] = $this->stream->readValue($column->type);
            }
        }

        if ($rowClass === null) {
            return $data;
        }

        /** @var \ArrayObject<string, mixed> $row */
        $row = new $rowClass($data);

        return $row;
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function getData(): ResultData {
        return $this->getRowsData();
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    #[\Override]
    public function getIterator(): ResultIterator {

        $clonedStream = clone $this->stream;
        $clonedStream->offset($this->dataOffset);

        return new ResultIterator(
            $clonedStream,
            $this->metadata,
            $this->rowCount,
            $this->dataOffset,
            $this->rowClass,
        );
    }

    #[\Override]
    public function getMetadata(): Metadata {
        return $this->metadata;
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function getRowsData(): RowsData {
        return new RowsData(
            rows: $this->fetchAll(),
        );
    }

    /**
     * @param \Cassandra\Metadata $metadata
     */
    #[\Override]
    public function setMetadata(Metadata $metadata): self {
        $this->metadata = $metadata;

        return $this;
    }

    /**
     * @param class-string<\Cassandra\Response\RowClass> $rowClass
     *
     * @throws \Cassandra\Response\Exception
     */
    public function setRowClass(string $rowClass): self {
        if (!is_subclass_of($rowClass, ArrayObject::class)) {
            throw new Exception('row class "' . $rowClass . '" is not a subclass of ArrayObject');
        }

        $this->rowClass = $rowClass;

        return $this;
    }

    #[\Override]
    protected function onPreviousResultUpdated(): void {
        if ($this->metadataOfPreviousResult !== null) {
            $this->metadata->mergeWithPreviousMetadata($this->metadataOfPreviousResult);
        }
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    protected function readRowsMetadata(): Metadata {
        $this->stream->offset(4);

        return $this->readMetadata(isPrepareMetaData: false);
    }
}
