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
     * @var class-string<RowClass> $rowClass
     */
    protected ?string $rowClass = null;

    protected int $rowCount = 0;

    /**
     * @throws \Cassandra\Response\Exception
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
     * @param class-string<RowClass> $rowClass
     * @return array<mixed>
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

        for ($i = 0; $i < $this->rowCount; ++$i) {
            $key = null;

            /** @psalm-suppress MixedAssignment */
            foreach ($this->metadata['columns'] as $j => $column) {
                $value = $this->stream->readValue($column['type']);

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
     * @param class-string<RowClass> $rowClass
     * @return ArrayObject<string, mixed>|array<string, mixed>|null
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function fetchRow(?string $rowClass = null): ArrayObject|array|null {

        $this->stream->offset($this->dataOffset);

        if ($this->rowCount === 0) {
            return null;
        }

        if ($rowClass === null) {
            $rowClass = $this->rowClass;
        }

        if ($rowClass !== null && !is_subclass_of($rowClass, ArrayObject::class)) {
            throw new Exception('row class "' . $rowClass . '" is not a subclass of ArrayObject');
        }

        $data = [];
        foreach ($this->metadata->columns as $column) {
            /** @psalm-suppress MixedAssignment */
            $data[$column->name] = $this->stream->readValue($column->type);
        }

        if ($rowClass === null) {
            return $data;
        }

        /** @var ArrayObject<string, mixed> $row */
        $row = new $rowClass($data);

        return $row;
    }

    public function getData(): ResultData {
        return $this->getRowsData();
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
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

    /**
     * @return \Cassandra\Metadata
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function getMetadata(): Metadata {
        return $this->metadata;
    }

    /*
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
    public function setMetadata(Metadata $metadata): static {
        $this->metadata = $metadata;

        return $this;
    }

    /**
     * @param class-string<RowClass> $rowClass
     *
     * @throws \Cassandra\Response\Exception
     */
    public function setRowClass(string $rowClass): static {
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
     */
    protected function readRowsMetadata(): Metadata {
        $this->stream->offset(4);

        return $this->readMetadata(isPrepareMetaData: false);
    }
}
