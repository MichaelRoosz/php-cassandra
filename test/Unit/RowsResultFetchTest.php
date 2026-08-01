<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ResponseException;
use Cassandra\Protocol\Header;
use Cassandra\Protocol\Opcode;
use Cassandra\Protocol\ProtocolVersion;
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

    public function testFetchKeyPairRejectsValueIndexOutsideTheResult(): void {
        $result = self::rowsResultWithOneColumn(Type::INT, [pack('N', 7)]);

        $this->expectException(ResponseException::class);
        $this->expectExceptionCode(ExceptionCode::RESPONSE_ROWS_INVALID_COLUMN_INDEX->value);

        $result->fetchKeyPair(0, 99);
    }

    private static function encodeString(string $value): string {
        return pack('n', strlen($value)) . $value;
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
