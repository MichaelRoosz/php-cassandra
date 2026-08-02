<?php

declare(strict_types=1);

namespace Cassandra\Response\Result;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ResponseException;
use Cassandra\Protocol\Header;
use Cassandra\Response\Result;
use Cassandra\Response\Result\Data\ResultData;
use Cassandra\Response\Result\Data\RowsData;
use Cassandra\Response\ResultFlag;
use Cassandra\Response\ResultIterator;
use Cassandra\Response\StreamReader;
use Cassandra\Value\ValueEncodeConfig;
use Throwable;

final class RowsResult extends Result {
    private int $dataOffsetOfPreviousRow;

    private int $fetchedRows = 0;

    /**
     * @var array{
     *     rowClass: class-string<\Cassandra\Response\Result\RowClassInterface>|null,
     *     constructorArgs: array<mixed>,
     *     fetchType: FetchType,
     * }
     */
    private array $fetchObjectConfiguration = [
        'rowClass' => null,
        'constructorArgs' => [],
        'fetchType' => FetchType::ASSOC,
    ];

    private int $rowCount = 0;

    private RowsMetadata $rowsMetadata;

    private ValueEncodeConfig $valueEncodeConfig;

    /**
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\TypeNameParserException
     * @throws \Cassandra\Exception\TypeInfoException
     */
    final public function __construct(Header $header, StreamReader $stream) {

        parent::__construct(
            header: $header,
            stream: $stream,
        );

        $this->stream->offset(4);
        $this->rowsMetadata = $this->readRowsMetadata();
        $this->rowCount = $this->stream->readInt();

        $this->assertRowCountFitsInBody();

        $this->dataOffset = $this->stream->pos();
        $this->dataOffsetOfPreviousRow = $this->dataOffset;

        $this->valueEncodeConfig = ValueEncodeConfig::default();
    }

    public function columnCount(): int {
        return $this->rowsMetadata->columnsCount;
    }

    /**
     * @param class-string<\Cassandra\Response\Result\RowClassInterface> $rowClass
     * @param array<mixed> $constructorArgs
     * @param FetchType $fetchType
     * 
     * @throws \Cassandra\Exception\ResponseException
     */
    public function configureFetchObject(string $rowClass, array $constructorArgs = [], FetchType $fetchType = FetchType::ASSOC): void {
        if (!is_subclass_of($rowClass, RowClassInterface::class)) {
            throw new ResponseException('Invalid row class for fetchObject: must implement \\Cassandra\\Response\\Result\\RowClassInterface', ExceptionCode::RESPONSE_ROWS_INVALID_ROWCLASS->value, [
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

    public function configureValueEncoding(ValueEncodeConfig $config): void {
        $this->valueEncodeConfig = $config;
    }

    /**
     * Fetches the next row from the result set.
     *
     * @return array<string|int, mixed>|false
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
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
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
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
     * The loop is driven by the row cursor rather than by fetchColumn()'s
     * false-for-no-more-rows sentinel, because that sentinel is indistinguishable
     * from a legitimate value: a boolean column decodes to PHP false, and testing
     * for it would end the result at the first false cell and silently drop every
     * row after it.
     *
     * @return array<mixed>
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    public function fetchAllColumns(int $index = 0): array {
        $values = [];
        while ($this->fetchedRows < $this->rowCount) {
            /** @psalm-suppress MixedAssignment */
            $values[] = $this->fetchColumn($index);
        }

        return $values;
    }

    /**
     * Fetches remaining rows and returns an associative map of key => value.
     * Consumes the cursor from the current position forward.
     *
     * @return array<int|string, mixed>
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    public function fetchAllKeyPairs(int $keyIndex = 0, int $valueIndex = 1, bool $mergeDuplicates = false): array {

        $columns = $this->rowsMetadata->columns;
        if ($columns === null) {
            throw new ResponseException('Column metadata is not available', ExceptionCode::RESPONSE_ROWS_NO_COLUMN_METADATA->value, [
                'operation' => 'RowsResult::fetchAllKeyPairs',
                'result_kind' => $this->kind->name,
            ]);
        }

        self::assertColumnIndex($keyIndex, count($columns), 'keyIndex', 'RowsResult::fetchAllKeyPairs');
        self::assertColumnIndex($valueIndex, count($columns), 'valueIndex', 'RowsResult::fetchAllKeyPairs');

        $map = [];
        $duplicateKeys = [];
        while (true) {
            if ($this->fetchedRows >= $this->rowCount) {
                break;
            }

            $key = null;
            $value = null;

            $previousOffset = $this->stream->pos();

            foreach ($columns as $j => $column) {
                /** @psalm-suppress MixedAssignment */
                $columnValue = $this->stream->readValue($column->type, $this->valueEncodeConfig);
                if ($j === $keyIndex) {
                    /** @psalm-suppress MixedAssignment */
                    $key = $columnValue;
                    if (!is_int($key) && !is_string($key)) {
                        throw new ResponseException('Invalid key type; expected string|int', ExceptionCode::RESPONSE_ROWS_INVALID_KEY_TYPE->value, [
                            'key_type' => gettype($key),
                            'key_index' => $keyIndex,
                        ]);
                    }
                }

                // Not an elseif: naming the same column as both key and value is
                // legitimate, and would otherwise yield a null value for every row.
                if ($j === $valueIndex) {
                    /** @psalm-suppress MixedAssignment */
                    $value = $columnValue;
                }
            }

            $this->dataOffsetOfPreviousRow = $previousOffset;
            $this->fetchedRows++;

            if ($key === null) {
                throw new ResponseException('Invalid key index', ExceptionCode::RESPONSE_ROWS_INVALID_KEY_INDEX->value, [
                    'operation' => 'RowsResult::fetchAllKeyPairs',
                    'key_index' => $keyIndex,
                    'column_count' => count($columns),
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
     * @return array<\Cassandra\Response\Result\RowClassInterface>
     *
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
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
     * Note that false is also a legitimate value of a boolean column, so it
     * cannot be used to detect the end of the result set; drive the loop off
     * {@see self::getRowCount()}, or use {@see self::fetchAllColumns()}, which
     * does.
     *
     * @return mixed|false
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    public function fetchColumn(int $index = 0): mixed {
        if ($this->fetchedRows >= $this->rowCount) {
            return false;
        }

        $columns = $this->rowsMetadata->columns;
        if ($columns === null) {
            throw new ResponseException('Column metadata is not available', ExceptionCode::RESPONSE_ROWS_NO_COLUMN_METADATA->value, [
                'operation' => 'RowsResult::fetchColumn',
                'result_kind' => $this->kind->name,
            ]);
        }

        self::assertColumnIndex($index, count($columns), 'index', 'RowsResult::fetchColumn');

        $previousOffset = $this->stream->pos();

        $value = null;
        foreach ($columns as $j => $column) {
            /** @psalm-suppress MixedAssignment */
            $cell = $this->stream->readValue($column->type, $this->valueEncodeConfig);
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
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    public function fetchKeyPair(int $keyIndex = 0, int $valueIndex = 1): array|false {
        if ($this->fetchedRows >= $this->rowCount) {
            return false;
        }

        $columns = $this->rowsMetadata->columns;
        if ($columns === null) {
            throw new ResponseException('Column metadata is not available', ExceptionCode::RESPONSE_ROWS_NO_COLUMN_METADATA->value, [
                'operation' => 'RowsResult::fetchKeyPair',
                'result_kind' => $this->kind->name,
            ]);
        }

        self::assertColumnIndex($keyIndex, count($columns), 'keyIndex', 'RowsResult::fetchKeyPair');
        self::assertColumnIndex($valueIndex, count($columns), 'valueIndex', 'RowsResult::fetchKeyPair');

        $previousOffset = $this->stream->pos();

        $key = null;
        $value = null;

        foreach ($columns as $j => $column) {
            /** @psalm-suppress MixedAssignment */
            $columnValue = $this->stream->readValue($column->type, $this->valueEncodeConfig);
            if ($j === $keyIndex) {
                /** @psalm-suppress MixedAssignment */
                $key = $columnValue;
                if (!is_int($key) && !is_string($key)) {
                    throw new ResponseException('Invalid key type; expected string|int', ExceptionCode::RESPONSE_ROWS_INVALID_KEY_TYPE->value, [
                        'key_type' => gettype($key),
                        'key_index' => $keyIndex,
                    ]);
                }
            }

            // Not an elseif: naming the same column as both key and value is
            // legitimate, and would otherwise yield a null value for every row.
            if ($j === $valueIndex) {
                /** @psalm-suppress MixedAssignment */
                $value = $columnValue;
            }
        }

        $this->dataOffsetOfPreviousRow = $previousOffset;
        $this->fetchedRows++;

        if ($key === null) {
            throw new ResponseException('Invalid key index', ExceptionCode::RESPONSE_ROWS_INVALID_KEY_INDEX->value, [
                'key_index' => $keyIndex,
                'column_count' => count($columns),
            ]);
        }

        return [$key => $value];
    }

    /**
     * Fetches the next row and returns it as an RowClassInterface instance.
     * Returns false when there are no more rows.
     *
     * @return \Cassandra\Response\Result\RowClassInterface|false
     * 
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    public function fetchObject(): RowClassInterface|false {

        $rowClass = $this->fetchObjectConfiguration['rowClass'] ?? RowClass::class;
        $additionalConstructorArgs = $this->fetchObjectConfiguration['constructorArgs'];
        $mode = $this->fetchObjectConfiguration['fetchType'];

        if (!is_subclass_of($rowClass, RowClassInterface::class)) {
            throw new ResponseException('row class "' . $rowClass . '" is not a subclass of \\Cassandra\\Response\\RowClassInterface', ExceptionCode::RESPONSE_ROWS_ROWCLASS_NOT_SUBCLASS->value, [
                'row_class' => $rowClass,
                'expected_interface' => RowClassInterface::class,
            ]);
        }

        $rowData = $this->fetch($mode);
        if ($rowData === false) {
            return false;
        }

        try {
            /** @var \Cassandra\Response\Result\RowClassInterface $row */
            $row = new $rowClass($rowData, $additionalConstructorArgs);
        } catch (Throwable $e) {
            throw new ResponseException(
                'Failed to construct configured row class',
                ExceptionCode::RESPONSE_ROWS_ROWCLASS_CONSTRUCTION_FAILED->value,
                [
                    'row_class' => $rowClass,
                ],
                $e
            );
        }

        return $row;
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    public function getData(): ResultData {
        return $this->getRowsData();
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
     */
    #[\Override]
    public function getIterator(): ResultIterator {

        $rowResult = clone $this;
        $rowResult->stream = clone $this->stream;

        $rowResult->rewind();

        return new ResultIterator($rowResult);
    }

    #[\Override]
    public function getRowCount(): int {
        return $this->rowCount;
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    public function getRowsData(): RowsData {

        $savedOffset = $this->stream->pos();
        $this->stream->offset($this->dataOffset);

        // Restored however this ends. This reads the whole result set from the
        // start without moving the fetch cursor, so a row that cannot be decoded
        // would otherwise leave the reader parked mid-stream while
        // {@see self::$fetchedRows} still says where the caller had got to — and
        // the next fetch() would hand them the middle of a row.
        try {
            $rows = [];
            for ($i = 0; $i < $this->rowCount; ++$i) {
                $rows[] = $this->readNextRow(FetchType::ASSOC);
            }
        } finally {
            $this->stream->offset($savedOffset);
        }

        return new RowsData(rows: $rows);
    }

    public function getRowsMetadata(): RowsMetadata {
        return $this->rowsMetadata;
    }

    public function hasMetadataChanged(): bool {
        return (bool) ($this->getRowsMetadata()->flags & ResultFlag::ROWS_FLAG_METADATA_CHANGED);
    }

    public function hasMorePages(): bool {
        return (bool) ($this->getRowsMetadata()->flags & ResultFlag::ROWS_FLAG_HAS_MORE_PAGES);
    }

    public function hasNoMetadata(): bool {
        return (bool) ($this->getRowsMetadata()->flags & ResultFlag::ROWS_FLAG_NO_METADATA);
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

    /**
     * @throws \Cassandra\Exception\ResponseException
     */
    public function rewind(): void {
        $this->fetchedRows = 0;
        $this->stream->offset($this->dataOffset);
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
     */
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
    protected function onPreviousRowsMetadataUpdated(RowsMetadata $previousRowsMetadata): void {
        $this->rowsMetadata = $this->rowsMetadata->mergeWithPreviousMetadata($previousRowsMetadata);
    }

    /**
     * Refuse a column index this result set does not have.
     *
     * The fetch helpers that take one pick their value out of the row by
     * comparing the index against the column position, so an index past the end
     * simply never matches and the cell they hand back stays null — which is
     * indistinguishable from a column that really is null. A caller who names
     * the wrong column gets nulls for every row rather than being told, and the
     * key index of the key-pair helpers gets it worse: the row is consumed
     * before the missing key is noticed.
     *
     * The key index is reported with its own code, which the key-pair helpers
     * also raise if a key still comes out null; see
     * {@see ExceptionCode::RESPONSE_ROWS_INVALID_KEY_INDEX}.
     *
     * @throws \Cassandra\Exception\ResponseException
     */
    private static function assertColumnIndex(int $index, int $columnCount, string $argument, string $operation): void {

        if ($index >= 0 && $index < $columnCount) {
            return;
        }

        throw new ResponseException(
            'Column index ' . $index . ' is outside this result set, which has ' . $columnCount . ' column(s)',
            $argument === 'keyIndex'
                ? ExceptionCode::RESPONSE_ROWS_INVALID_KEY_INDEX->value
                : ExceptionCode::RESPONSE_ROWS_INVALID_COLUMN_INDEX->value,
            [
                'operation' => $operation,
                'argument' => $argument,
                'index' => $index,
                'column_count' => $columnCount,
            ]
        );
    }

    /**
     * Refuse a row count the frame body cannot possibly hold.
     *
     * The count is four bytes the peer chose, and everything that walks the
     * result set is driven by it: {@see self::fetchAll()},
     * {@see self::getRowsData()} and {@see self::fetchAllColumns()} all build an
     * array with one entry per row. A cell is at least its four-byte length
     * prefix, so a row is at least four bytes per column, and a count past what
     * is left of the body is a number no well-formed frame can mean.
     *
     * Without this the arithmetic has a hole in it exactly where it hurts: a
     * result declaring no columns has rows of no bytes at all, so a sixteen-byte
     * body can announce two billion of them and the walkers will dutifully build
     * two billion entries for it. There is no such result — a SELECT has at least
     * one column — so it is refused rather than bounded.
     *
     * Checked here rather than left to the readers to run out of data, because
     * by then the memory has already been asked for; the same reasoning as
     * {@see \Cassandra\Connection\ResponseReader::MAX_FRAME_BODY_LENGTH} and
     * {@see \Cassandra\Response\StreamReader::MAX_TYPE_NESTING_DEPTH}.
     *
     * @throws \Cassandra\Exception\ResponseException
     */
    private function assertRowCountFitsInBody(): void {

        $columnsCount = $this->rowsMetadata->columnsCount;
        $remainingLength = $this->stream->remainingLength();

        // A [int] on the wire, so both of these are signed and a corrupt frame
        // can make either negative.
        $maximumRowCount = ($this->rowCount < 0 || $columnsCount < 0)
            ? -1
            : ($columnsCount === 0 ? 0 : intdiv($remainingLength, $columnsCount * 4));

        if ($maximumRowCount >= 0 && $this->rowCount <= $maximumRowCount) {
            return;
        }

        throw new ResponseException(
            'Result declares more rows than its frame body can hold',
            ExceptionCode::RESPONSE_ROWS_ROW_COUNT_OUT_OF_RANGE->value,
            [
                'operation' => 'RowsResult::__construct',
                'row_count' => $this->rowCount,
                'max_row_count' => $maximumRowCount,
                'columns_count' => $columnsCount,
                'remaining_body_length' => $remainingLength,
            ]
        );
    }

    /**
     * @return array<mixed>
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    private function readNextRow(FetchType $mode = FetchType::ASSOC): array {
        if ($this->rowsMetadata->columns === null) {
            throw new ResponseException('Column metadata is not available', ExceptionCode::RESPONSE_ROWS_NO_COLUMN_METADATA->value, [
                'operation' => 'RowsResult::readNextRow',
                'result_kind' => $this->kind->name,
            ]);
        }

        $row = [];

        switch ($mode) {
            case FetchType::ASSOC:
                foreach ($this->rowsMetadata->columns as $column) {
                    /** @psalm-suppress MixedAssignment */
                    $row[$column->name] = $this->stream->readValue($column->type, $this->valueEncodeConfig);
                }

                break;

            case FetchType::NUM:
                foreach ($this->rowsMetadata->columns as $column) {
                    /** @psalm-suppress MixedAssignment */
                    $row[] = $this->stream->readValue($column->type, $this->valueEncodeConfig);
                }

                break;

            case FetchType::BOTH:
                // The two views share one key space, because PHP folds a column
                // name that is a canonical decimal integer string — a quoted
                // numeric identifier such as "0" — into the very int a position
                // uses. Appending the positional value would then put it at
                // whatever key such a name left free, and every position after it
                // would be off by one; that is why the index is written
                // explicitly here and why the positional view is written again
                // below, which puts back anything a later name overwrote.
                //
                // Settled in the positional view's favour because that is the one
                // that has to be complete and in step with the columns: a caller
                // reading $row[2] means the third column, whereas a name that
                // spells a number is only ever reachable as that same key anyway.
                // Re-assigning an existing key leaves it where it is, so the
                // interleaved order of the row is unchanged.
                $positional = [];

                foreach ($this->rowsMetadata->columns as $index => $column) {
                    /** @psalm-suppress MixedAssignment */
                    $value = $this->stream->readValue($column->type, $this->valueEncodeConfig);

                    /** @psalm-suppress MixedAssignment */
                    $row[$column->name] = $value;

                    /** @psalm-suppress MixedAssignment */
                    $row[$index] = $value;

                    /** @psalm-suppress MixedAssignment */
                    $positional[$index] = $value;
                }

                /** @psalm-suppress MixedAssignment */
                foreach ($positional as $index => $value) {
                    $row[$index] = $value;
                }

                break;
        }

        return $row;
    }
}
