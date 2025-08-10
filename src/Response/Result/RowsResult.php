<?php

declare(strict_types=1);

namespace Cassandra\Response\Result;

use Cassandra\Protocol\Header;
use Cassandra\Response\Exception;
use Cassandra\Response\Result;
use Cassandra\Response\Result\Data\ResultData;
use Cassandra\Response\Result\Data\RowsData;
use Cassandra\Response\ResultFlag;
use Cassandra\Response\ResultIterator;
use Cassandra\Response\RowClass;
use Cassandra\Response\RowClassInterface;
use Cassandra\Response\StreamReader;

final class RowsResult extends Result {
    private int $dataOffsetOfPreviousRow;

    private int $fetchedRows = 0;

    /**
     * @var array{
     *     rowClass: class-string<\Cassandra\Response\RowClassInterface>|null,
     *     constructorArgs: array<mixed>,
     *     fetchType: FetchType,
     * }
     */
    private array $fetchObjectConfiguration = [
        'rowClass' => null,
        'constructorArgs' => [],
        'fetchType' => FetchType::ASSOC,
    ];

    private Metadata $metadata;

    private int $rowCount = 0;

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
        $this->dataOffsetOfPreviousRow = $this->dataOffset;
    }

    public function columnCount(): int {
        return $this->metadata->columnsCount;
    }

    /**
     * @param class-string<\Cassandra\Response\RowClassInterface> $rowClass
     * @param array<mixed> $constructorArgs
     * @param FetchType $fetchType
     * 
     * @throws \Cassandra\Response\Exception
     */
    public function configureFetchObject(string $rowClass, array $constructorArgs = [], FetchType $fetchType = FetchType::ASSOC): void {
        if (!is_subclass_of($rowClass, RowClassInterface::class)) {
            throw new Exception('Invalid row class for fetchObject: must implement RowClassInterface', Exception::ROWS_INVALID_ROWCLASS, [
                'operation' => 'RowsResult::configureFetchObject',
                'row_class' => $rowClass,
                'expected_interface' => RowClassInterface::class,
            ]);
        }

        $this->fetchObjectConfiguration = [
            'rowClass' => $rowClass,
            'constructorArgs' => $constructorArgs,
            'fetchType' => $fetchType,
        ];
    }

    /**
     * Fetches the next row from the result set.
     *
     * @return array<string|int, mixed>|false
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function fetch(FetchType $mode = FetchType::ASSOC): array|false {
        if ($this->fetchedRows >= $this->rowCount) {
            return false;
        }

        $previousOffset = $this->stream->pos();

        $row = $this->readNextRow($mode);

        $this->dataOffsetOfPreviousRow = $previousOffset;
        $this->fetchedRows++;

        return $row;
    }

    /**
     * Fetches the remaining rows from the current cursor position.
     *
     * @return array<int, array<string|int, mixed>>
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function fetchAll(FetchType $mode = FetchType::ASSOC): array {
        $rows = [];
        while (true) {
            $row = $this->fetch($mode);
            if ($row === false) {
                break;
            }
            $rows[] = $row;
        }

        return $rows;
    }

    /**
     * Fetches the remaining rows from the current cursor position and returns
     * the value of the specified column for each row. Behaves like fetchAll()
     * in that it consumes the stream from the current cursor forward.
     *
     * @return array<mixed>
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function fetchAllColumns(int $index = 0): array {
        $values = [];
        while (true) {
            /** @psalm-suppress MixedAssignment */
            $value = $this->fetchColumn($index);
            if ($value === false) {
                break;
            }

            /** @psalm-suppress MixedAssignment */
            $values[] = $value;
        }

        return $values;
    }

    /**
     * Fetches remaining rows and returns an associative map of key => value.
     * Consumes the cursor from the current position forward.
     *
     * @return array<int|string, mixed>
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function fetchAllKeyPairs(int $keyIndex = 0, int $valueIndex = 1, bool $mergeDuplicates = false): array {

        if ($this->metadata->columns === null) {
            throw new Exception('Column metadata is not available', Exception::ROWS_NO_COLUMN_METADATA, [
                'operation' => 'RowsResult::fetchAllKeyPairs',
                'result_kind' => $this->kind->name,
            ]);
        }

        $map = [];
        $duplicateKeys = [];
        while (true) {
            if ($this->fetchedRows >= $this->rowCount) {
                break;
            }

            $key = null;
            $value = null;

            $previousOffset = $this->stream->pos();

            foreach ($this->metadata->columns as $j => $column) {
                /** @psalm-suppress MixedAssignment */
                $columnValue = $this->stream->readValue($column->type);
                if ($j === $keyIndex) {
                    /** @psalm-suppress MixedAssignment */
                    $key = $columnValue;
                    if (!is_int($key) && !is_string($key)) {
                        throw new Exception('Invalid key type; expected string|int', Exception::ROWS_INVALID_KEY_TYPE, [
                            'key_type' => gettype($key),
                            'key_index' => $keyIndex,
                        ]);
                    }
                } elseif ($j === $valueIndex) {
                    /** @psalm-suppress MixedAssignment */
                    $value = $columnValue;
                }
            }

            $this->dataOffsetOfPreviousRow = $previousOffset;
            $this->fetchedRows++;

            if ($key === null) {
                throw new Exception('Invalid key index', Exception::ROWS_INVALID_KEY_INDEX, [
                    'operation' => 'RowsResult::fetchAllKeyPairs',
                    'key_index' => $keyIndex,
                    'column_count' => count($this->metadata->columns),
                ]);
            }

            if ($mergeDuplicates) {
                if (array_key_exists($key, $map)) {
                    if (!isset($duplicateKeys[$key]) || !is_array($map[$key])) {
                        $map[$key] = [$map[$key], $value];
                        $duplicateKeys[$key] = true;
                    } else {
                        /** @psalm-suppress MixedAssignment */
                        $map[$key][] = $value;
                    }
                } else {
                    /** @psalm-suppress MixedAssignment */
                    $map[$key] = $value;
                }
            } else {
                /** @psalm-suppress MixedAssignment */
                $map[$key] = $value;
            }
        }

        return $map;
    }

    /**
     * Fetches all remaining rows and returns them as RowClassInterface instances.
     *
     * @return array<RowClassInterface>
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function fetchAllObjects(): array {

        $rows = [];
        while (true) {
            $row = $this->fetchObject();
            if ($row === false) {
                break;
            }
            $rows[] = $row;
        }

        return $rows;
    }

    /**
     * Returns a single column from the next row of a result set.
     * Returns false when there are no more rows.
     *
     * @return mixed|false
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function fetchColumn(int $index = 0): mixed {
        if ($this->fetchedRows >= $this->rowCount) {
            return false;
        }

        if ($this->metadata->columns === null) {
            throw new Exception('Column metadata is not available', Exception::ROWS_NO_COLUMN_METADATA, [
                'operation' => 'RowsResult::fetchColumn',
                'result_kind' => $this->kind->name,
            ]);
        }

        $previousOffset = $this->stream->pos();

        $value = null;
        foreach ($this->metadata->columns as $j => $column) {
            /** @psalm-suppress MixedAssignment */
            $cell = $this->stream->readValue($column->type);
            if ($j === $index) {
                /** @psalm-suppress MixedAssignment */
                $value = $cell;
            }
        }

        $this->dataOffsetOfPreviousRow = $previousOffset;
        $this->fetchedRows++;

        return $value;
    }

    /**
     * Fetches a single key/value pair from the next row.
     * Returns false when there are no more rows.
     *
     * @return array<int|string, mixed>|false
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function fetchKeyPair(int $keyIndex = 0, int $valueIndex = 1): array|false {
        if ($this->fetchedRows >= $this->rowCount) {
            return false;
        }

        if ($this->metadata->columns === null) {
            throw new Exception('Column metadata is not available', Exception::ROWS_NO_COLUMN_METADATA, [
                'operation' => 'RowsResult::fetchKeyPair',
                'result_kind' => $this->kind->name,
            ]);
        }

        $previousOffset = $this->stream->pos();

        $key = null;
        $value = null;

        foreach ($this->metadata->columns as $j => $column) {
            /** @psalm-suppress MixedAssignment */
            $columnValue = $this->stream->readValue($column->type);
            if ($j === $keyIndex) {
                /** @psalm-suppress MixedAssignment */
                $key = $columnValue;
                if (!is_int($key) && !is_string($key)) {
                    throw new Exception('Invalid key type; expected string|int', Exception::ROWS_INVALID_KEY_TYPE, [
                        'key_type' => gettype($key),
                        'key_index' => $keyIndex,
                    ]);
                }
            } elseif ($j === $valueIndex) {
                /** @psalm-suppress MixedAssignment */
                $value = $columnValue;
            }
        }

        $this->dataOffsetOfPreviousRow = $previousOffset;
        $this->fetchedRows++;

        if ($key === null) {
            throw new Exception('Invalid key index', Exception::ROWS_INVALID_KEY_INDEX, [
                'key_index' => $keyIndex,
                'column_count' => count($this->metadata->columns),
            ]);
        }

        return [$key => $value];
    }

    /**
     * Fetches the next row and returns it as an RowClassInterface instance.
     * Returns false when there are no more rows.
     *
     * @return RowClassInterface|false
     * 
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function fetchObject(): RowClassInterface|false {

        $rowClass = $this->fetchObjectConfiguration['rowClass'] ?? RowClass::class;
        $additionalConstructorArgs = $this->fetchObjectConfiguration['constructorArgs'];
        $mode = $this->fetchObjectConfiguration['fetchType'];

        if (!is_subclass_of($rowClass, RowClassInterface::class)) {
            throw new Exception('row class "' . $rowClass . '" is not a subclass of \\Cassandra\\Response\\RowClassInterface', Exception::ROWS_ROWCLASS_NOT_SUBCLASS, [
                'row_class' => $rowClass,
                'expected_interface' => RowClassInterface::class,
            ]);
        }

        $rowData = $this->fetch($mode);
        if ($rowData === false) {
            return false;
        }

        /** @var RowClassInterface $row */
        $row = new $rowClass($rowData, $additionalConstructorArgs);

        return $row;
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function getData(): ResultData {
        return $this->getRowsData();
    }

    #[\Override]
    public function getIterator(): ResultIterator {

        $rowResult = clone $this;
        $rowResult->stream = clone $this->stream;
        $rowResult->stream->offset($rowResult->dataOffset);

        return new ResultIterator($rowResult);
    }

    #[\Override]
    public function getMetadata(): Metadata {
        return $this->metadata;
    }

    #[\Override]
    public function getRowCount(): int {
        return $this->rowCount;
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function getRowsData(): RowsData {

        $savedOffset = $this->stream->pos();
        $this->stream->offset($this->dataOffset);

        $rows = [];
        for ($i = 0; $i < $this->rowCount; ++$i) {
            $rows[] = $this->readNextRow(FetchType::ASSOC);
        }

        $this->stream->offset($savedOffset);

        return new RowsData(rows: $rows);
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function hasMetadataChanged(): bool {
        return (bool) ($this->getMetadata()->flags & ResultFlag::ROWS_FLAG_METADATA_CHANGED->value);
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function hasMorePages(): bool {
        return (bool) ($this->getMetadata()->flags & ResultFlag::ROWS_FLAG_HAS_MORE_PAGES->value);
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function hasNoMetadata(): bool {
        return (bool) ($this->getMetadata()->flags & ResultFlag::ROWS_FLAG_NO_METADATA->value);
    }

    public function isFetchObjectConfigurationSet(): bool {
        return $this->fetchObjectConfiguration['rowClass'] !== null;
    }

    public function resetFetchObjectConfiguration(): void {
        $this->fetchObjectConfiguration = [
            'rowClass' => null,
            'constructorArgs' => [],
            'fetchType' => FetchType::ASSOC,
        ];
    }

    public function rewind(): void {
        $this->fetchedRows = 0;
        $this->stream->offset($this->dataOffset);
    }

    public function rewindOneRow(): void {

        if ($this->fetchedRows < 1) {
            return;
        }

        $this->fetchedRows--;
        $this->stream->offset($this->dataOffsetOfPreviousRow);
    }

    public function rowCount(): int {
        return $this->rowCount;
    }

    #[\Override]
    protected function onPreviousResultUpdated(): void {
        if ($this->metadataOfPreviousResult !== null) {
            $this->metadata = $this->metadata->mergeWithPreviousMetadata($this->metadataOfPreviousResult);
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

    /**
     * @return array<mixed>
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    private function readNextRow(FetchType $mode = FetchType::ASSOC): array {
        if ($this->metadata->columns === null) {
            throw new Exception('Column metadata is not available', Exception::ROWS_NO_COLUMN_METADATA, [
                'operation' => 'RowsResult::readNextRow',
                'result_kind' => $this->kind->name,
            ]);
        }

        $row = [];

        switch ($mode) {
            case FetchType::ASSOC:
                foreach ($this->metadata->columns as $column) {
                    /** @psalm-suppress MixedAssignment */
                    $row[$column->name] = $this->stream->readValue($column->type);
                }

                break;

            case FetchType::NUM:
                foreach ($this->metadata->columns as $column) {
                    /** @psalm-suppress MixedAssignment */
                    $row[] = $this->stream->readValue($column->type);
                }

                break;

            case FetchType::BOTH:
                foreach ($this->metadata->columns as $column) {
                    /** @psalm-suppress MixedAssignment */
                    $value = $this->stream->readValue($column->type);

                    /** @psalm-suppress MixedAssignment */
                    $row[$column->name] = $value;

                    /** @psalm-suppress MixedAssignment */
                    $row[] = $value;
                }

                break;
        }

        return $row;
    }
}
