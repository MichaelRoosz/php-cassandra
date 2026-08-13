<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\Type;
use Cassandra\TypeInfo\ListCollectionInfo;
use Cassandra\TypeInfo\MapCollectionInfo;
use Cassandra\TypeInfo\SetCollectionInfo;
use Cassandra\TypeInfo\SimpleTypeInfo;
use Cassandra\TypeInfo\TupleInfo;
use Cassandra\TypeInfo\TypeInfo;
use Cassandra\TypeInfo\UDTInfo;
use Cassandra\Value\SetCollection;
use Cassandra\Value\ValueComparator;

/**
 * A binary that is not a valid serialization of the type named beside it has to
 * be reported as a ValueException.
 *
 * ValueComparator is only ever fed binaries the collection encoders have just
 * produced, so its reads used to take the framing on trust: unpack() on a
 * truncated buffer, and direct indexing into strings assumed to be sixteen bytes
 * long. Neither of those raises a project exception — unpack() emits an
 * E_WARNING and returns false, and a read past the end of a string emits another
 * — so an application whose error handler promotes warnings (Symfony's and
 * Laravel's do) got a native ErrorException out of the library, or a TypeError
 * once the false propagated. That is the failure
 * {@see \Cassandra\Value\ValueWithFixedLength::assertExactBinaryLength()} exists
 * to prevent everywhere else.
 *
 * The compare() entry point is public, so these binaries are reachable; the
 * encoders themselves cannot produce one, which is why the second half of this
 * class checks that the guards cost the well-formed path nothing.
 */
final class ValueComparatorMalformedBinaryTest extends AbstractUnitTestCase {
    /**
     * Identical binaries are answered before anything is read, malformed or not.
     * That is the fast path the guards must not have moved: two encodings that
     * are byte-for-byte equal are equal whatever they mean.
     */
    public function testIdenticalBinariesAreEqualWithoutBeingParsed(): void {
        $truncated = "\x00\x00";

        $this->assertSame(0, ValueComparator::compare(
            new ListCollectionInfo(valueType: new SimpleTypeInfo(Type::INT), isFrozen: false),
            $truncated,
            $truncated,
        ));
    }

    /**
     * @dataProvider truncatedProvider
     */
    public function testTruncatedBinaryIsReported(TypeInfo $typeInfo, string $left, string $right): void {
        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_COMPARATOR_TRUNCATED_BINARY->value);

        ValueComparator::compare($typeInfo, $left, $right);
    }

    /**
     * The encoders sort and de-duplicate through compare(), so a set that
     * encodes at all is the evidence that the guards let well-formed binaries
     * through — for the fixed-width types the new width checks apply to.
     *
     * @dataProvider wellFormedSetProvider
     *
     * @param array<mixed> $elements
     */
    public function testWellFormedValuesStillCompareAndEncode(Type $type, array $elements): void {
        $typeInfo = new SetCollectionInfo(valueType: new SimpleTypeInfo($type), isFrozen: false);

        $binary = (new SetCollection($elements, $typeInfo))->getBinary();

        $decoded = SetCollection::fromBinary($binary, $typeInfo);

        $this->assertCount(count($elements), $decoded->getValue());
        $this->assertSame($binary, $decoded->getBinary(), 'a decoded set re-encodes to the same ordered bytes');
    }

    /**
     * @dataProvider wrongWidthProvider
     */
    public function testWrongWidthBinaryIsReported(TypeInfo $typeInfo, string $left, string $right): void {
        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_COMPARATOR_INVALID_LENGTH->value);

        ValueComparator::compare($typeInfo, $left, $right);
    }
    /**
     * Every shape whose framing runs past the end of the buffer that carries it.
     *
     * @return array<string, array{0: TypeInfo, 1: string, 2: string}>
     */
    public static function truncatedProvider(): array {
        $int = new SimpleTypeInfo(Type::INT);
        $text = new SimpleTypeInfo(Type::VARCHAR);

        return [
            'list: element count cut short' => [
                new ListCollectionInfo(valueType: $int, isFrozen: false),
                "\x00\x00",
                "\x00\x00\x00",
            ],
            'set: element count promises an element that is not there' => [
                new SetCollectionInfo(valueType: $int, isFrozen: false),
                "\x00\x00\x00\x01",
                "\x00\x00\x00\x02",
            ],
            'map: entry length runs past the end' => [
                new MapCollectionInfo(keyType: $text, valueType: $int, isFrozen: false),
                "\x00\x00\x00\x01" . "\x00\x00\x00\x7f" . 'ab',
                "\x00\x00\x00\x01" . "\x00\x00\x00\x7e" . 'ab',
            ],
            'tuple: field length cut short' => [
                new TupleInfo(valueTypes: [$int, $text]),
                "\x00\x00\x00",
                "\x00\x00",
            ],
            'udt: field length runs past the end' => [
                new UDTInfo(valueTypes: ['a' => $int], isFrozen: false, keyspace: 'k', name: 'u'),
                "\x00\x00\x00\x40" . 'xy',
                "\x00\x00\x00\x41" . 'xy',
            ],
            'signed integer: no byte to take the sign from' => [
                new SimpleTypeInfo(Type::BIGINT),
                '',
                "\x00\x00\x00\x00\x00\x00\x00\x01",
            ],
        ];
    }

    /**
     * @return array<string, array{0: Type, 1: array<mixed>}>
     */
    public static function wellFormedSetProvider(): array {
        return [
            'double' => [Type::DOUBLE, [1.5, -0.5, 0.0, 1000.25]],
            'float' => [Type::FLOAT, [1.5, -0.5, 0.0]],
            'int' => [Type::INT, [3, -1, 0, 2147483647]],
            'bigint' => [Type::BIGINT, [3, -1, 0, PHP_INT_MAX]],
            'uuid' => [Type::UUID, [
                '5a2b1c3d-4e5f-4a6b-8c9d-0e1f2a3b4c5d',
                'b1e2c3d4-5566-4788-99aa-bbccddeeff00',
            ]],
            'timeuuid' => [Type::TIMEUUID, [
                '5a2b1c3d-4e5f-1a6b-8c9d-0e1f2a3b4c5d',
                'b1e2c3d4-5566-1788-99aa-bbccddeeff00',
            ]],
            'text' => [Type::VARCHAR, ['b', 'a', 'c']],
        ];
    }

    /**
     * Every shape whose binary is not the width its fixed-width type is
     * serialized at.
     *
     * @return array<string, array{0: TypeInfo, 1: string, 2: string}>
     */
    public static function wrongWidthProvider(): array {
        return [
            'double: seven bytes' => [new SimpleTypeInfo(Type::DOUBLE), str_repeat("\x01", 7), str_repeat("\x02", 7)],
            'double: empty against eight' => [new SimpleTypeInfo(Type::DOUBLE), '', str_repeat("\x00", 8)],
            'float: three bytes' => [new SimpleTypeInfo(Type::FLOAT), str_repeat("\x01", 3), str_repeat("\x02", 3)],
            'float: eight bytes' => [new SimpleTypeInfo(Type::FLOAT), str_repeat("\x01", 8), str_repeat("\x02", 8)],
            'uuid: six bytes' => [new SimpleTypeInfo(Type::UUID), str_repeat("\x01", 6), str_repeat("\x02", 6)],
            'uuid: fifteen against sixteen' => [new SimpleTypeInfo(Type::UUID), str_repeat("\x01", 15), str_repeat("\x02", 16)],
            'timeuuid: seven bytes' => [new SimpleTypeInfo(Type::TIMEUUID), str_repeat("\x11", 7), str_repeat("\x12", 7)],
        ];
    }
}
