<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Consistency;
use Cassandra\Exception\CassandraException;
use Cassandra\Exception\RequestException;
use Cassandra\Exception\ResponseException;
use Cassandra\Exception\ValueFactoryException;
use Cassandra\Protocol\Flag;
use Cassandra\Protocol\Header;
use Cassandra\Protocol\Opcode;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Execute;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Request\Query;
use Cassandra\Response\Result\PreparedResult;
use Cassandra\Response\Result\RowsResult;
use Cassandra\Response\StreamReader;
use Cassandra\Test\Support\ScriptedNode;
use Cassandra\Type;
use Cassandra\TypeInfo\ListCollectionInfo;
use Cassandra\TypeInfo\MapCollectionInfo;
use Cassandra\TypeInfo\SimpleTypeInfo;
use Cassandra\TypeInfo\TupleInfo;
use Cassandra\Value;
use Cassandra\Value\EncodeOption\MapEncodeOption;
use Cassandra\Value\ValueComparator;
use Cassandra\Value\ValueEncodeConfig;
use Cassandra\ValueFactory;
use Closure;

/**
 * Orderings the comparator has to get right for frozen collections, map keys
 * that PHP arrays cannot hold apart, and request encodings that do not match.
 */
final class ValueSideRemainingTest extends AbstractUnitTestCase {
    protected function setUp(): void {
        parent::setUp();

        if (PHP_INT_SIZE < 8) {
            $this->markTestSkipped('needs 64-bit integers');
        }
    }

    public function testComparatorOrderings(): void {
        $int = new SimpleTypeInfo(Type::INT);
        $compare = static fn (\Cassandra\TypeInfo\TypeInfo $type, Value\ValueBase $left, Value\ValueBase $right): int => ValueComparator::compare($type, $left->getBinary(), $right->getBinary());

        $this->assertSame(-1, $compare(new SimpleTypeInfo(Type::DECIMAL), Value\Decimal::fromValue('-1'), Value\Decimal::fromValue('1')));
        $this->assertSame(1, $compare(new SimpleTypeInfo(Type::DECIMAL), Value\Decimal::fromValue('-1'), Value\Decimal::fromValue('-2')));
        $this->assertSame(-1, $compare(new SimpleTypeInfo(Type::VARINT), Value\Varint::fromValue(5), Value\Varint::fromValue(7)) <=> 0);

        $this->assertSame(1, $compare(new SimpleTypeInfo(Type::DOUBLE), Value\Double::fromValue(NAN), Value\Double::fromValue(1.0)));
        $this->assertSame(-1, $compare(new SimpleTypeInfo(Type::DOUBLE), Value\Double::fromValue(1.0), Value\Double::fromValue(NAN)));
        $this->assertSame(-1, $compare(new SimpleTypeInfo(Type::FLOAT), Value\Float32::fromValue(1.0), Value\Float32::fromValue(2.0)) <=> 0);

        $this->assertNotSame(0, $compare(new SimpleTypeInfo(Type::INET), Value\Inet::fromValue('10.0.0.1'), Value\Inet::fromValue('::1')));

        $v1a = Value\Uuid::fromValue('8db96410-8dba-11f0-b0eb-325096b39f47');
        $v1b = Value\Uuid::fromValue('8db96410-8dba-11f0-b0eb-325096b39f48');
        $v4 = Value\Uuid::fromValue('550e8400-e29b-41d4-a716-446655440000');
        $this->assertSame(-1, $compare(new SimpleTypeInfo(Type::UUID), $v1a, $v1b) <=> 0);
        $this->assertSame(-1, $compare(new SimpleTypeInfo(Type::UUID), $v1a, $v4));

        $list = new ListCollectionInfo($int, false);
        $this->assertSame(-1, $compare($list, Value\ListCollection::fromValue([1], Type::INT), Value\ListCollection::fromValue([1, 2], Type::INT)));
        $this->assertSame(-1, $compare($list, Value\ListCollection::fromValue([], Type::INT), Value\ListCollection::fromValue([1], Type::INT)));

        $map = new MapCollectionInfo($int, $int, false);
        $this->assertSame(-1, $compare($map, Value\MapCollection::fromValue([1 => 2], Type::INT, Type::INT), Value\MapCollection::fromValue([1 => 3], Type::INT, Type::INT)));

        $tuple = new TupleInfo([$int, $int]);
        $this->assertSame(-1, $compare($tuple, Value\Tuple::fromValue([null, 1], [Type::INT, Type::INT]), Value\Tuple::fromValue([1, 1], [Type::INT, Type::INT])));
        $this->assertSame(1, $compare($tuple, Value\Tuple::fromValue([1, null], [Type::INT, Type::INT]), Value\Tuple::fromValue([1, null], [Type::INT, Type::INT])) + 1);

        // Type infos that do not describe the type fall back to a byte comparison.
        foreach ([Type::LIST, Type::SET, Type::MAP, Type::TUPLE, Type::UDT] as $type) {
            $this->assertSame(-1, ValueComparator::compare(new SimpleTypeInfo($type), 'a', 'b') <=> 0);
        }
    }

    public function testMapKeysThatPhpArraysCannotHoldApart(): void {
        $double = new MapCollectionInfo(new SimpleTypeInfo(Type::DOUBLE), new SimpleTypeInfo(Type::INT), false);
        $read = static fn (string $binary, MapEncodeOption $option): mixed => Value\MapCollection::fromBinary($binary, $double)->asConfigured(new ValueEncodeConfig(mapEncodeOption: $option));

        $special = Value\MapCollection::fromValue(['NAN' => 1, 'INF' => 2, '-INF' => 3], Type::DOUBLE, Type::INT);
        $array = $read($special->getBinary(), MapEncodeOption::AUTO);
        $this->assertIsArray($array);
        $this->assertEqualsCanonicalizing(['NAN', 'INF', '-INF'], array_keys($array));

        $zeros = Value\MapCollection::fromEntries([new Value\MapEntry(0.0, 1), new Value\MapEntry(-0.0, 2)], Type::DOUBLE, Type::INT);
        // Distinct CQL keys stay distinct PHP keys, so nothing is lost.
        $this->assertCount(2, (array) $read($zeros->getBinary(), MapEncodeOption::AS_ARRAY));

        $objectKeys = Value\MapCollection::fromEntries([new Value\MapEntry(Value\Int32::fromValue(1), 2)], Type::INT, Type::INT);
        $this->assertNotSame('', $objectKeys->getBinary());
    }

    public function testReadingRowsByAnIndexThatDoesNotExist(): void {
        $rows = $this->rows();
        $this->assertRaises(ResponseException::class, static fn () => $rows->fetchAllKeyPairs(5, 0));
        $rows->rewind();
        $this->assertRaises(ResponseException::class, static fn () => $rows->fetchKeyPair(5, 0));
    }

    public function testRequestEncodingMismatches(): void {
        $encode = static function (\Cassandra\Request\Request $request): string {
            $request->setVersion(ProtocolVersion::V4);
            $request->setStream(1);

            return (string) $request;
        };

        $this->assertRaises(RequestException::class, static fn () => $encode(new Query('SELECT ?', [1], Consistency::ONE, new QueryOptions(autoPrepare: false, namesForValues: true))));
        $this->assertRaises(RequestException::class, static fn () => $encode(new Query('SELECT :a', ['a' => 1], Consistency::ONE, new QueryOptions(autoPrepare: false, namesForValues: false))));

        $query = new Query('SELECT 1');
        $query->setFlags(Flag::CUSTOM_PAYLOAD);
        $this->assertNotSame('', $encode($query));

        // An unterminated quoted marker is read to the end of the statement.
        $this->assertNotSame('', $encode(new Query('SELECT :"unterminated', ['unterminated' => 1], Consistency::ONE, new QueryOptions(autoPrepare: false))));

        // Two values match an unquoted marker case-insensitively and neither exactly.
        $prepared = $this->preparedWithMarker('xy');
        $this->assertRaises(RequestException::class, static fn () => new Execute($prepared, ['Xy' => 1, 'XY' => 2], Consistency::ONE, new ExecuteOptions(namesForValues: true)));
    }

    public function testStreamReaderResynchronises(): void {
        $config = new ValueEncodeConfig();
        $duration = Value\Duration::fromValue('1h')->getBinary();

        // A decoder that stops short of the cell skips the rest; one that runs past it is refused.
        $reader = new StreamReader(pack('N', strlen($duration) + 2) . $duration . 'xx' . pack('N', 4) . pack('N', 9));
        $reader->readValue(new SimpleTypeInfo(Type::DURATION), $config);
        $this->assertSame(9, $reader->readValue(new SimpleTypeInfo(Type::INT), $config));
        $this->assertRaises(ResponseException::class, static fn () => (new StreamReader(pack('N', 1) . $duration))->readValue(new SimpleTypeInfo(Type::DURATION), $config));

        foreach ([Type::LIST, Type::SET] as $type) {
            $this->assertRaises(CassandraException::class, static fn () => (new StreamReader(pack('N', 4) . pack('N', 0)))->readValue(new SimpleTypeInfo($type), $config));
        }
    }

    public function testValueFactoryRefusals(): void {
        $this->assertRaises(ValueFactoryException::class, static fn () => ValueFactory::getTypeInfoFromUnvalidatedDefinition(['x' => 1]));
        $this->assertRaises(ValueFactoryException::class, static fn () => ValueFactory::getTypeInfoFromUnvalidatedDefinition(['type' => 'int']));
    }

    /**
     * @param class-string<CassandraException> $expected
     * @param Closure(): mixed $call
     */
    private function assertRaises(string $expected, Closure $call): void {
        try {
            $call();
            $this->fail("expected $expected");
        } catch (CassandraException $e) {
            $this->assertInstanceOf($expected, $e, $e->getMessage());
        }
    }

    private function preparedWithMarker(string $name): PreparedResult {
        $body = pack('N', 4) . pack('n', 2) . 'id' . pack('NNN', 0x0001, 1, 0) . ScriptedNode::string('ks') . ScriptedNode::string('t')
            . ScriptedNode::string($name) . pack('n', 0x0009) . pack('NN', 0x0004, 0);

        return new PreparedResult(new Header(ProtocolVersion::V4, 0, 1, Opcode::RESPONSE_RESULT, strlen($body)), new StreamReader($body));
    }

    private function rows(): RowsResult {
        $body = pack('N', 2) . pack('NN', 0x0001, 1) . ScriptedNode::string('ks') . ScriptedNode::string('t') . ScriptedNode::string('c') . pack('n', 9)
            . pack('N', 1) . pack('N', 4) . pack('N', 1);

        return new RowsResult(new Header(ProtocolVersion::V4, 0, 1, Opcode::RESPONSE_RESULT, strlen($body)), new StreamReader($body));
    }
}
