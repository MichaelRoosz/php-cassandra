<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\Type;
use Cassandra\Value\ListCollection;
use Cassandra\Value\MapCollection;
use Cassandra\Value\SetCollection;
use Cassandra\Value\Tuple;
use Cassandra\Value\UDT;
use Cassandra\Value\Vector;
use Cassandra\Response\StreamReader;
use Cassandra\TypeInfo\ListCollectionInfo;
use Cassandra\TypeInfo\MapCollectionInfo;
use Cassandra\TypeInfo\SetCollectionInfo;
use Cassandra\TypeInfo\SimpleTypeInfo;
use Cassandra\Value\ValueEncodeConfig;

/**
 * CQL has no null inside a list, set, map or vector, so encoding one is refused
 * — but it has to be refused as the null it is.
 *
 * Without an explicit check the null reached ValueFactory::getValueObjectFromValue(),
 * which reports a null value object as "Cannot get type object for value": a
 * ValueFactoryException about the type system, for what is a plain fact about
 * the data. Tuples and UDTs are the other way about — a null field is
 * legitimate there and is encoded as a -1 length — so they are checked here too,
 * to keep the two apart.
 */
final class CollectionNullElementTest extends AbstractUnitTestCase {
    public function testAListObjectCannotBeDecodedWithANullElement(): void {
        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_LIST_NULL_ELEMENT->value);

        ListCollection::fromBinary(
            pack('N', 1) . pack('N', -1),
            new ListCollectionInfo(new SimpleTypeInfo(Type::INT), false),
        );
    }
    public function testAListWithANullElementIsRefusedAsSuch(): void {

        $list = ListCollection::fromValue(['a', null, 'c'], Type::VARCHAR);

        try {
            $list->getBinary();
            $this->fail('expected the null element to be refused');
        } catch (ValueException $e) {
            $this->assertSame(ExceptionCode::VALUE_LIST_NULL_ELEMENT->value, $e->getCode());
            $this->assertSame(1, $e->getContext()['index'] ?? null);
            $this->assertStringContainsString('cannot be null', $e->getMessage());
        }
    }

    public function testAMapObjectCannotBeDecodedWithANullValue(): void {
        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_MAP_NULL_VALUE->value);

        MapCollection::fromBinary(
            pack('N', 1) . pack('N', 4) . pack('N', 1) . pack('N', -1),
            new MapCollectionInfo(
                new SimpleTypeInfo(Type::INT),
                new SimpleTypeInfo(Type::INT),
                false,
            ),
        );
    }

    public function testAMapWithANullValueIsRefusedAsSuch(): void {

        $map = MapCollection::fromValue(['a' => 1, 'b' => null], Type::VARCHAR, Type::INT);

        try {
            $map->getBinary();
            $this->fail('expected the null value to be refused');
        } catch (ValueException $e) {
            $this->assertSame(ExceptionCode::VALUE_MAP_NULL_VALUE->value, $e->getCode());
            $this->assertSame('b', $e->getContext()['key'] ?? null);
        }
    }

    public function testASetObjectCannotBeDecodedWithANullElement(): void {
        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_SET_NULL_ELEMENT->value);

        SetCollection::fromBinary(
            pack('N', 1) . pack('N', -1),
            new SetCollectionInfo(new SimpleTypeInfo(Type::INT), false),
        );
    }

    public function testASetWithANullElementIsRefusedAsSuch(): void {

        $set = SetCollection::fromValue([1, null], Type::INT);

        try {
            $set->getBinary();
            $this->fail('expected the null element to be refused');
        } catch (ValueException $e) {
            $this->assertSame(ExceptionCode::VALUE_SET_NULL_ELEMENT->value, $e->getCode());
            $this->assertSame(1, $e->getContext()['index'] ?? null);
        }
    }

    public function testATupleWithANullFieldStillEncodes(): void {
        // A null tuple field is legitimate CQL and goes out as a -1 length.
        $tuple = Tuple::fromValue([1, null], [Type::INT, Type::VARCHAR]);

        $this->assertSame(
            '00000004' . '00000001' . 'ffffffff',
            bin2hex($tuple->getBinary())
        );
    }

    public function testAUdtWithANullFieldStillEncodes(): void {
        // As for tuples: an absent UDT field is a null field, not an error.
        $udt = UDT::fromValue(['a' => 1, 'b' => null], ['a' => Type::INT, 'b' => Type::VARCHAR]);

        $this->assertSame(
            '00000004' . '00000001' . 'ffffffff',
            bin2hex($udt->getBinary())
        );
    }

    public function testAVectorWithANullElementIsRefusedAsSuch(): void {

        $vector = Vector::fromValue([1.0, null, 3.0], Type::FLOAT, 3);

        try {
            $vector->getBinary();
            $this->fail('expected the null element to be refused');
        } catch (ValueException $e) {
            $this->assertSame(ExceptionCode::VALUE_VECTOR_NULL_ELEMENT->value, $e->getCode());
            $this->assertSame(1, $e->getContext()['index'] ?? null);
        }
    }

    public function testCollectionsWithoutNullsAreUnaffected(): void {

        $this->assertSame(
            '00000002' . '00000001' . '61' . '00000001' . '62',
            bin2hex(ListCollection::fromValue(['a', 'b'], Type::VARCHAR)->getBinary())
        );

        $this->assertSame(
            '00000001' . '00000001' . '61' . '00000004' . '00000001',
            bin2hex(MapCollection::fromValue(['a' => 1], Type::VARCHAR, Type::INT)->getBinary())
        );
    }

    public function testTheFastListDecoderRejectsANullElement(): void {
        $body = pack('N', 1) . pack('N', -1);
        $reader = new StreamReader(pack('N', strlen($body)) . $body);

        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_LIST_NULL_ELEMENT->value);

        $reader->readValue(
            new ListCollectionInfo(new SimpleTypeInfo(Type::INT), false),
            ValueEncodeConfig::default(),
        );
    }

    public function testTheFastSetDecoderRejectsANullElement(): void {
        $body = pack('N', 1) . pack('N', -1);
        $reader = new StreamReader(pack('N', strlen($body)) . $body);

        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_SET_NULL_ELEMENT->value);

        $reader->readValue(
            new SetCollectionInfo(new SimpleTypeInfo(Type::INT), false),
            ValueEncodeConfig::default(),
        );
    }
}
