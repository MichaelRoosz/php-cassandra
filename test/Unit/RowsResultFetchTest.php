<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ResponseException;
use Cassandra\Protocol\Header;
use Cassandra\Protocol\Opcode;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Response\Result\FetchType;
use Cassandra\Response\Result\RowClassInterface;
use Cassandra\Response\Result\RowsResult;
use Cassandra\Response\StreamReader;
use Cassandra\Type;

class RowsResultFetchTest extends AbstractUnitTestCase {
    public function testFetchAllColumnsKeepsFalseValues(): void {
        // The regression this exists for: fetchColumn() reports "no more rows"
        // as false, which is also what a boolean column decodes to, so a
        // fetchAllColumns() that tested for the sentinel ended the result at the
        // first false cell and silently dropped every row after it.
        $result = self::rowsResultWithOneColumn(Type::BOOLEAN, [
            chr(1),
            chr(0),
            chr(1),
            chr(0),
        ]);

        $this->assertSame([true, false, true, false], $result->fetchAllColumns());
    }

    public function testFetchAllColumnsReturnsEveryRow(): void {
        $result = self::rowsResultWithOneColumn(Type::INT, [
            pack('N', 7),
            pack('N', 8),
            pack('N', 9),
        ]);

        $this->assertSame([7, 8, 9], $result->fetchAllColumns());
        $this->assertSame([], $result->fetchAllColumns(), 'the cursor is consumed');

        $result->rewind();
        $this->assertSame([7, 8, 9], $result->fetchAllColumns());
    }

    public function testFetchAllKeyPairsAcceptsTheSameColumnAsKeyAndValue(): void {
        // Legitimate, and used to yield a null value for every row because the
        // value was only picked up in an elseif of the key's branch.
        $result = self::rowsResultWithOneColumn(Type::INT, [
            pack('N', 7),
            pack('N', 8),
        ]);

        $this->assertSame([7 => 7, 8 => 8], $result->fetchAllKeyPairs(0, 0));
    }

    public function testFetchAllKeyPairsFailureLeavesCursorAtFailedRow(): void {
        $result = self::rowsResultWithOneColumn(Type::BOOLEAN, [chr(0), chr(1)]);

        try {
            $result->fetchAllKeyPairs(0, 0);
            $this->fail('Expected the boolean key to be rejected');
        } catch (ResponseException $e) {
            $this->assertSame(ExceptionCode::RESPONSE_ROWS_INVALID_KEY_TYPE->value, $e->getCode());
        }

        $this->assertSame(['col' => false], $result->fetch());
        $this->assertSame(['col' => true], $result->fetch());
    }

    public function testFetchAllKeyPairsRejectsIndexOutsideTheResult(): void {
        $result = self::rowsResultWithOneColumn(Type::INT, [pack('N', 7)]);

        $this->expectException(ResponseException::class);
        $this->expectExceptionCode(ExceptionCode::RESPONSE_ROWS_INVALID_COLUMN_INDEX->value);

        $result->fetchAllKeyPairs(0, 99);
    }

    public function testFetchAllKeyPairsRejectsKeyIndexOutsideTheResult(): void {
        $result = self::rowsResultWithOneColumn(Type::INT, [pack('N', 7)]);

        $this->expectException(ResponseException::class);
        $this->expectExceptionCode(ExceptionCode::RESPONSE_ROWS_INVALID_KEY_INDEX->value);

        $result->fetchAllKeyPairs(99, 0);
    }

    public function testFetchBothKeepsPositionalKeysInStepWithTheColumns(): void {
        // A quoted numeric column name is folded by PHP into the very int key a
        // position uses, so appending the positional value put it at whatever key
        // such a name left free and shifted every position after it by one.
        $result = self::rowsResultWithColumns(
            ['0', 'name', 'other'],
            Type::INT,
            [pack('N', 7), pack('N', 8), pack('N', 9)],
        );

        $row = $result->fetch(FetchType::BOTH);

        $this->assertIsArray($row);

        // Every column reachable at its own position, and the names beside them.
        $this->assertSame(7, $row[0]);
        $this->assertSame(8, $row[1]);
        $this->assertSame(9, $row[2]);
        $this->assertSame(8, $row['name']);
        $this->assertSame(9, $row['other']);

        // And where the collision is with a position another column already
        // holds, the positional view is the one that survives it.
        $result = self::rowsResultWithColumns(
            ['name', '0'],
            Type::INT,
            [pack('N', 7), pack('N', 8)],
        );

        $row = $result->fetch(FetchType::BOTH);

        $this->assertIsArray($row);
        $this->assertSame(7, $row[0]);
        $this->assertSame(8, $row[1]);
        $this->assertSame(7, $row['name']);
    }

    public function testFetchBothKeepsTheDocumentedKeyOrder(): void {
        // The README spells the row out as ['id' => …, 0 => …, 'name' => …, 1 => …],
        // so the two views stay interleaved rather than being appended in blocks.
        $result = self::rowsResultWithColumns(
            ['id', 'name'],
            Type::INT,
            [pack('N', 7), pack('N', 8)],
        );

        $row = $result->fetch(FetchType::BOTH);

        $this->assertIsArray($row);
        $this->assertSame(['id', 0, 'name', 1], array_keys($row));
        $this->assertSame([7, 7, 8, 8], array_values($row));
    }

    public function testFetchColumnFailureLeavesCursorAtFailedRow(): void {
        $result = self::rowsResultWithOneColumn(Type::INT, ["\x00\x00\x01", pack('N', 7)]);

        $this->assertSameDecodeFailureTwice(static fn () => $result->fetchColumn());
    }

    public function testFetchColumnRejectsIndexOutsideTheResult(): void {
        // Without the bounds check this returned null, which is what a column
        // that really is null returns: a caller naming the wrong column was
        // handed nulls for every row rather than being told.
        $result = self::rowsResultWithOneColumn(Type::INT, [pack('N', 7)]);

        $this->expectException(ResponseException::class);
        $this->expectExceptionCode(ExceptionCode::RESPONSE_ROWS_INVALID_COLUMN_INDEX->value);

        $result->fetchColumn(99);
    }

    public function testFetchColumnRejectsNegativeIndex(): void {
        $result = self::rowsResultWithOneColumn(Type::INT, [pack('N', 7)]);

        $this->expectException(ResponseException::class);
        $this->expectExceptionCode(ExceptionCode::RESPONSE_ROWS_INVALID_COLUMN_INDEX->value);

        $result->fetchColumn(-1);
    }

    public function testFetchColumnReturnsFalseOnlyPastTheLastRow(): void {
        $result = self::rowsResultWithOneColumn(Type::BOOLEAN, [chr(0)]);

        $this->assertFalse($result->fetchColumn(), 'the row holds false');
        $this->assertFalse($result->fetchColumn(), 'and so does the end of the result');
        $this->assertSame(1, $result->getRowCount());
    }

    public function testFetchFailureLeavesCursorAtFailedRow(): void {
        $result = self::rowsResultWithOneColumn(Type::INT, ["\x00\x00\x01", pack('N', 7)]);

        $this->assertSameDecodeFailureTwice(static fn () => $result->fetch());
    }

    public function testFetchKeyPairFailureLeavesCursorAtFailedRow(): void {
        $result = self::rowsResultWithOneColumn(Type::BOOLEAN, [chr(0), chr(1)]);

        try {
            $result->fetchKeyPair(0, 0);
            $this->fail('Expected the boolean key to be rejected');
        } catch (ResponseException $e) {
            $this->assertSame(ExceptionCode::RESPONSE_ROWS_INVALID_KEY_TYPE->value, $e->getCode());
        }

        $this->assertSame(['col' => false], $result->fetch());
        $this->assertSame(['col' => true], $result->fetch());
    }

    public function testFetchKeyPairRejectsValueIndexOutsideTheResult(): void {
        $result = self::rowsResultWithOneColumn(Type::INT, [pack('N', 7)]);

        $this->expectException(ResponseException::class);
        $this->expectExceptionCode(ExceptionCode::RESPONSE_ROWS_INVALID_COLUMN_INDEX->value);

        $result->fetchKeyPair(0, 99);
    }

    public function testFetchObjectWrapsRowConstructorFailure(): void {
        $result = self::rowsResultWithOneColumn(Type::INT, [pack('N', 7)]);
        $result->configureFetchObject(FailingRowClass::class);

        try {
            $result->fetchObject();
            $this->fail('Expected row construction to fail');
        } catch (ResponseException $e) {
            $this->assertSame(ExceptionCode::RESPONSE_ROWS_ROWCLASS_CONSTRUCTION_FAILED->value, $e->getCode());
            $this->assertSame(FailingRowClass::class, $e->context()['row_class']);
            $this->assertInstanceOf(\Error::class, $e->getPrevious());
            $this->assertSame('row construction failed', $e->getPrevious()->getMessage());
        }
    }

    public function testGetIteratorWalksEveryRowAfterAPartialFetch(): void {
        // getIterator() hands back a clone, which carries this result's fetch
        // cursor as well as its reader. Putting only the reader back to the
        // first row left the two disagreeing about where the caller had got to,
        // and the iterator then stopped as many rows early as had been fetched —
        // handing back false for each of them rather than the rows the reader
        // was in fact positioned on.
        $result = self::rowsResultWithOneColumn(Type::INT, [
            pack('N', 7),
            pack('N', 8),
            pack('N', 9),
        ]);

        $this->assertSame(['col' => 7], $result->fetch());
        $this->assertSame(['col' => 8], $result->fetch());

        // Driven by hand rather than with foreach(), which calls rewind() of its
        // own accord and so puts the two back in step whatever getIterator() did.
        $iterator = $result->getIterator();

        $rows = [];
        while ($iterator->valid()) {
            $rows[$iterator->key()] = $iterator->current();
            $iterator->next();
        }

        $this->assertSame([['col' => 7], ['col' => 8], ['col' => 9]], $rows);

        // And foreach() over the same result still walks all of it.
        $rows = [];
        $keys = [];
        foreach ($result as $key => $row) {
            $keys[] = $key;
            $rows[] = $row;
        }

        $this->assertSame([0, 1, 2], $keys);
        $this->assertSame([['col' => 7], ['col' => 8], ['col' => 9]], $rows);
    }

    public function testHugeNoMetadataColumnCountIsRejectedWithoutIntegerOverflow(): void {
        $body = pack('N', 2) // result kind: ROWS
            . pack('N', 4) // flags: NO_METADATA
            . pack('N', 2147483647) // column count
            . pack('N', 1); // row count

        $header = new Header(
            version: ProtocolVersion::V4,
            flags: 0,
            stream: 1,
            opcode: Opcode::RESPONSE_RESULT,
            length: strlen($body),
        );

        $this->expectException(ResponseException::class);
        $this->expectExceptionCode(ExceptionCode::RESPONSE_ROWS_ROW_COUNT_OUT_OF_RANGE->value);

        new RowsResult($header, new StreamReader($body));
    }

    /**
     * @param callable(): mixed $fetch
     */
    private function assertSameDecodeFailureTwice(callable $fetch): void {
        for ($attempt = 0; $attempt < 2; ++$attempt) {
            try {
                $fetch();
                $this->fail('Expected malformed fixed-width value to be rejected');
            } catch (ResponseException $e) {
                $this->assertSame(ExceptionCode::RESPONSE_SR_VALUE_LENGTH_MISMATCH->value, $e->getCode());
                $this->assertSame(3, $e->context()['declared_length']);
            }
        }
    }

    private static function encodeString(string $value): string {
        return pack('n', strlen($value)) . $value;
    }

    /**
     * A ROWS result of one row, with one named column per cell.
     *
     * @param array<string> $columnNames
     * @param array<string> $cells one serialized value per column
     *
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\TypeNameParserException
     */
    private static function rowsResultWithColumns(array $columnNames, Type $type, array $cells): RowsResult {

        $body = pack('N', 2) // result kind: ROWS
            . pack('N', 1) // flags: GLOBAL_TABLES_SPEC
            . pack('N', count($columnNames)) // column count
            . self::encodeString('testks')
            . self::encodeString('testtable');

        foreach ($columnNames as $columnName) {
            $body .= self::encodeString($columnName) . pack('n', $type->value);
        }

        $body .= pack('N', 1); // row count

        foreach ($cells as $cell) {
            $body .= pack('N', strlen($cell)) . $cell;
        }

        $header = new Header(
            version: ProtocolVersion::V4,
            flags: 0,
            stream: 1,
            opcode: Opcode::RESPONSE_RESULT,
            length: strlen($body),
        );

        return new RowsResult($header, new StreamReader($body));
    }

    /**
     * A ROWS result of one column, with one cell per row.
     *
     * @param array<string> $cells one serialized value per row
     *
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\TypeNameParserException
     */
    private static function rowsResultWithOneColumn(Type $type, array $cells): RowsResult {

        $body = pack('N', 2) // result kind: ROWS
            . pack('N', 1) // flags: GLOBAL_TABLES_SPEC
            . pack('N', 1) // column count
            . self::encodeString('testks')
            . self::encodeString('testtable')
            . self::encodeString('col')
            . pack('n', $type->value)
            . pack('N', count($cells));

        foreach ($cells as $cell) {
            $body .= pack('N', strlen($cell)) . $cell;
        }

        $header = new Header(
            version: ProtocolVersion::V4,
            flags: 0,
            stream: 1,
            opcode: Opcode::RESPONSE_RESULT,
            length: strlen($body),
        );

        return new RowsResult($header, new StreamReader($body));
    }
}

final class FailingRowClass implements RowClassInterface {
    public function __construct(array $rowData, array $additionalArguments = []) {
        throw new \Error('row construction failed');
    }
}
