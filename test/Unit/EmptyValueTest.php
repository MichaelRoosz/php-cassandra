<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Response\StreamReader;
use Cassandra\Type;
use Cassandra\TypeInfo\ListCollectionInfo;
use Cassandra\TypeInfo\SimpleTypeInfo;
use Cassandra\TypeInfo\TupleInfo;
use Cassandra\Value\ValueEncodeConfig;
use PHPUnit\Framework\Attributes\DataProvider;

/**
 * Regression: a fixed-length decoder handed an empty cell read on into the next
 * cell, returning its bytes and desynchronizing the rest of the row.
 */
final class EmptyValueTest extends AbstractUnitTestCase {
    #[DataProvider('types')]
    public function testAnEmptyCellLeavesTheNextCellIntact(\Cassandra\TypeInfo\TypeInfo $type, mixed $expected): void {
        $config = new ValueEncodeConfig();
        $reader = new StreamReader(pack('N', 0) . pack('N', 4) . pack('N', 9));

        $this->assertSame($expected, $reader->readValue($type, $config));
        $this->assertSame(9, $reader->readValue(new SimpleTypeInfo(Type::INT), $config));
    }
    /**
     * @return array<string, array{\Cassandra\TypeInfo\TypeInfo, mixed}>
     */
    public static function types(): array {
        $int = new SimpleTypeInfo(Type::INT);

        return [
            'int' => [$int, null],
            'bigint' => [new SimpleTypeInfo(Type::BIGINT), null],
            'boolean' => [new SimpleTypeInfo(Type::BOOLEAN), null],
            'double' => [new SimpleTypeInfo(Type::DOUBLE), null],
            'uuid' => [new SimpleTypeInfo(Type::UUID), null],
            'timestamp' => [new SimpleTypeInfo(Type::TIMESTAMP), null],
            'inet' => [new SimpleTypeInfo(Type::INET), null],
            'decimal' => [new SimpleTypeInfo(Type::DECIMAL), null],
            'text' => [new SimpleTypeInfo(Type::VARCHAR), ''],
            'blob' => [new SimpleTypeInfo(Type::BLOB), ''],
            'list' => [new ListCollectionInfo($int, false), []],
            'tuple' => [new TupleInfo([$int, $int]), [null, null]],
        ];
    }
}
