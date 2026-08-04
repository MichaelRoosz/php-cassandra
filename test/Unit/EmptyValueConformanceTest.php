<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Exception\CassandraException;
use Cassandra\Response\StreamReader;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;
use Cassandra\Value\ValueEncodeConfig;
use Cassandra\ValueFactory;

/**
 * What a zero-length ("empty") value means is declared per type by
 * `ValueBase::allowsEmpty()` and `ValueBase::isEmptyValueMeaningless()`, after
 * Cassandra's AbstractType. Two things then read those declarations: the direct
 * decoder `fromBinary()`, and the row decoder behind
 * `StreamReader::readValue()`, which resolves an empty cell from its length
 * before any decoder sees it.
 *
 * The two used to be written out separately — a hand-maintained match in
 * StreamReader against ad-hoc per-class handling — with nothing tying them
 * together, which is how they came to disagree (an empty varint decoded as 0
 * directly and as null in a row; an empty boolean as false and as null). This
 * pins them to each other and to the declarations, for every type at once, so a
 * type added or changed later cannot quietly drift.
 *
 * The row path deliberately keeps its match rather than deriving the answer at
 * runtime: it runs per cell, and a class lookup plus two static calls there
 * would cost real time on a large result set. This test is what makes that
 * safe.
 */
final class EmptyValueConformanceTest extends AbstractUnitTestCase {
    /**
     * Where a type does admit an empty value, the row path and the direct
     * decoder must produce the same thing — null for the types whose empty
     * value denotes null, and the same value object's value for the types where
     * it is a value of its own.
     *
     * @dataProvider typesAdmittingEmptyProvider
     */
    public function testEmptyCellAgreesWithTheDirectDecoder(Type $type): void {
        $typeInfo = self::typeInfoFor($type);

        $valueObject = ValueFactory::getValueObjectFromBinary($typeInfo, '');

        if (ValueFactory::isEmptyValueMeaningless($type)) {
            $this->assertNull($valueObject, $type->name . ': fromBinary(\'\') must report null');
            $this->assertNull(self::readEmptyCell($typeInfo), $type->name . ': an empty cell must decode to null');

            return;
        }

        $this->assertNotNull($valueObject, $type->name . ': an empty value is a value of its own here');
        $this->assertSame(
            $valueObject->getValue(),
            self::readEmptyCell($typeInfo),
            $type->name . ': the row path and fromBinary() must agree on an empty value'
        );
    }

    /**
     * A type that does not admit an empty value refuses one from the direct
     * decoder — Cassandra's own serializers do, `ShortSerializer::validate()`
     * demanding "2 bytes for a smallint" and `CollectionType::validate()`
     * refusing an empty collection outright.
     *
     * The row path stays lenient about the same cell rather than failing the
     * whole page over one malformed value, and this pins that too: whatever it
     * reports, it must not raise and must not consume any of the body, since
     * every value after it depends on the cell being zero bytes wide.
     *
     * @dataProvider typesForbiddingEmptyProvider
     */
    public function testTypeThatForbidsEmptyRefusesItDirectlyButNotInARow(Type $type): void {
        $typeInfo = self::typeInfoFor($type);

        try {
            ValueFactory::getValueObjectFromBinary($typeInfo, '');
            $this->fail($type->name . ': fromBinary(\'\') must refuse an empty value');
        } catch (CassandraException) {
            $this->addToAssertionCount(1);
        }

        // an empty cell, then an int cell holding 4242
        $reader = new StreamReader(pack('N', 0) . pack('N', 4) . pack('N', 4242));
        $reader->readValue($typeInfo, new ValueEncodeConfig());

        $this->assertSame(4, $reader->pos(), $type->name . ': an empty cell must consume exactly zero bytes of body');
        $this->assertSame(
            4242,
            $reader->readValue(ValueFactory::getTypeInfoFromType(Type::INT), new ValueEncodeConfig()),
            $type->name . ': the value after an empty cell must still decode'
        );
    }
    /**
     * @return array<string, array{Type}>
     *
     * @throws \Cassandra\Exception\CassandraException
     */
    public static function typesAdmittingEmptyProvider(): array {
        return self::typesWhere(true);
    }

    /**
     * @return array<string, array{Type}>
     *
     * @throws \Cassandra\Exception\CassandraException
     */
    public static function typesForbiddingEmptyProvider(): array {
        return self::typesWhere(false);
    }

    /**
     * Read a single zero-length cell through the row decoder.
     *
     * @throws \Cassandra\Exception\CassandraException
     */
    private static function readEmptyCell(TypeInfo $typeInfo): mixed {
        return (new StreamReader(pack('N', 0)))->readValue($typeInfo, new ValueEncodeConfig());
    }

    /**
     * @throws \Cassandra\Exception\CassandraException
     */
    private static function typeInfoFor(Type $type): TypeInfo {
        return match ($type) {
            Type::CUSTOM => ValueFactory::getTypeInfoFromTypeDefinition([
                'type' => Type::CUSTOM,
                'javaClassName' => 'java.lang.String',
            ]),
            Type::LIST => ValueFactory::getTypeInfoFromTypeDefinition([
                'type' => Type::LIST,
                'valueType' => Type::INT,
            ]),
            Type::SET => ValueFactory::getTypeInfoFromTypeDefinition([
                'type' => Type::SET,
                'valueType' => Type::INT,
            ]),
            Type::MAP => ValueFactory::getTypeInfoFromTypeDefinition([
                'type' => Type::MAP,
                'keyType' => Type::VARCHAR,
                'valueType' => Type::INT,
            ]),
            Type::TUPLE => ValueFactory::getTypeInfoFromTypeDefinition([
                'type' => Type::TUPLE,
                'valueTypes' => [Type::INT, Type::VARCHAR],
            ]),
            Type::UDT => ValueFactory::getTypeInfoFromTypeDefinition([
                'type' => Type::UDT,
                'valueTypes' => ['a' => Type::INT, 'b' => Type::VARCHAR],
            ]),
            Type::VECTOR => ValueFactory::getTypeInfoFromTypeDefinition([
                'type' => Type::VECTOR,
                'valueType' => Type::FLOAT,
                'dimensions' => 3,
            ]),
            default => ValueFactory::getTypeInfoFromType($type),
        };
    }

    /**
     * Every type whose allowsEmpty() is $admitsEmpty, so that the two tests
     * between them cover the whole of Type::cases() and neither has to skip.
     *
     * @return array<string, array{Type}>
     *
     * @throws \Cassandra\Exception\CassandraException
     */
    private static function typesWhere(bool $admitsEmpty): array {
        $cases = [];
        foreach (Type::cases() as $type) {
            if (ValueFactory::allowsEmptyValue($type) === $admitsEmpty) {
                $cases[strtolower($type->name)] = [$type];
            }
        }

        return $cases;
    }
}
