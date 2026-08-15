<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\Response\StreamReader;
use Cassandra\Type;
use Cassandra\TypeInfo\ListCollectionInfo;
use Cassandra\TypeInfo\MapCollectionInfo;
use Cassandra\TypeInfo\SetCollectionInfo;
use Cassandra\TypeInfo\SimpleTypeInfo;
use Cassandra\TypeInfo\VectorInfo;
use Cassandra\Value\ListCollection;
use Cassandra\Value\MapCollection;
use Cassandra\Value\SetCollection;
use Cassandra\Value\ValueEncodeConfig;
use Cassandra\Value\Vector;

/**
 * A collection body is held to the length its own cell declares, not to what is
 * left of the frame.
 *
 * The element count is four bytes the peer chose, and the decoders build one PHP
 * array entry per element before anything has been read towards them. Bounding
 * that by the reader alone is not enough for a value inside a row: what is left
 * there is the rest of the whole frame, so a cell declaring eight bytes could
 * have the driver build an array sized by a 256 MB frame — and
 * {@see \Cassandra\Response\StreamReader::resyncAfterValue()} would only report
 * the over-read afterwards, once the memory had already been asked for.
 *
 * {@see \Cassandra\Response\StreamReader::readCollectionValues()}, the fast path
 * a row's own list and set cells take, has always applied the tighter bound;
 * these are the object-path decoders, which every map takes and which a list or
 * set nested in a tuple or UDT takes.
 */
final class CollectionCountBoundTest extends AbstractUnitTestCase {
    public function testAListCountIsHeldToItsDeclaredCellLength(): void {
        // One entry's worth of cell, a count claiming a thousand, and a frame
        // that goes on long enough to satisfy them.
        $stream = new StreamReader(pack('N', 1000) . str_repeat(pack('N', 4) . pack('N', 7), 1000));

        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_LIST_INVALID_VALUE_TYPE->value);

        ListCollection::fromStream(
            $stream,
            length: 4 + 8,
            typeInfo: new ListCollectionInfo(new SimpleTypeInfo(Type::INT), false),
            valueEncodeConfig: ValueEncodeConfig::default(),
        );
    }

    public function testAListThatFitsItsDeclaredCellLengthIsStillRead(): void {
        $body = pack('N', 2) . pack('N', 4) . pack('N', 7) . pack('N', 4) . pack('N', 9);
        $stream = new StreamReader($body . str_repeat("\x00", 512));

        $list = ListCollection::fromStream(
            $stream,
            length: strlen($body),
            typeInfo: new ListCollectionInfo(new SimpleTypeInfo(Type::INT), false),
            valueEncodeConfig: ValueEncodeConfig::default(),
        );

        $this->assertSame([7, 9], $list->getValue());
    }

    public function testAMapCountIsHeldToItsDeclaredCellLength(): void {
        $entry = pack('N', 4) . pack('N', 7) . pack('N', 4) . pack('N', 9);
        $stream = new StreamReader(pack('N', 1000) . str_repeat($entry, 1000));

        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_MAP_INVALID_VALUE_TYPE->value);

        MapCollection::fromStream(
            $stream,
            length: 4 + 16,
            typeInfo: new MapCollectionInfo(
                new SimpleTypeInfo(Type::INT),
                new SimpleTypeInfo(Type::INT),
                false,
            ),
            valueEncodeConfig: ValueEncodeConfig::default(),
        );
    }

    public function testASetCountIsHeldToItsDeclaredCellLength(): void {
        $stream = new StreamReader(pack('N', 1000) . str_repeat(pack('N', 4) . pack('N', 7), 1000));

        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_SET_INVALID_VALUE_TYPE->value);

        SetCollection::fromStream(
            $stream,
            length: 4 + 8,
            typeInfo: new SetCollectionInfo(new SimpleTypeInfo(Type::INT), false),
            valueEncodeConfig: ValueEncodeConfig::default(),
        );
    }

    public function testAStandaloneCollectionBinaryIsBoundedByItself(): void {
        // fromBinary() hands the decoder a reader holding exactly the value, so
        // there is no enclosing cell and the reader is the whole of the bound.
        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_LIST_INVALID_VALUE_TYPE->value);

        ListCollection::fromBinary(
            pack('N', 1000) . pack('N', 4) . pack('N', 7),
            new ListCollectionInfo(new SimpleTypeInfo(Type::INT), false),
        );
    }

    public function testAStandaloneVectorBinaryIsBoundedByItself(): void {
        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_VECTOR_TRUNCATED_VALUE->value);

        Vector::fromBinary(
            pack('G', 1.0) . pack('G', 2.0),
            new VectorInfo(new SimpleTypeInfo(Type::FLOAT), 3),
        );
    }

    /**
     * A vector carries no count at all — the dimensions come from its type — so
     * a cell that stops short of them is the one case the count bound above
     * cannot catch. It used to be read on into whatever followed, which
     * {@see \Cassandra\Response\StreamReader::resyncAfterValue()} then reported
     * as a value that overran its cell: true, but saying nothing about the
     * vector it was, and only after the following cells had been consumed.
     */
    public function testAVectorOfFixedLengthElementsIsHeldToItsDeclaredCellLength(): void {
        // Two floats' worth of cell for a vector that declares three.
        $stream = new StreamReader(pack('G', 1.0) . pack('G', 2.0) . pack('N', 4) . pack('N', 4242));

        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_VECTOR_TRUNCATED_VALUE->value);

        Vector::fromStream(
            $stream,
            length: 8,
            typeInfo: new VectorInfo(new SimpleTypeInfo(Type::FLOAT), 3),
            valueEncodeConfig: ValueEncodeConfig::default(),
        );
    }

    public function testAVectorOfVariableLengthElementsIsHeldToItsDeclaredCellLength(): void {
        // One element's worth of cell for a vector that declares two.
        $stream = new StreamReader(chr(1) . 'a' . pack('N', 4) . pack('N', 4242));

        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_VECTOR_TRUNCATED_VALUE->value);

        Vector::fromStream(
            $stream,
            length: 2,
            typeInfo: new VectorInfo(new SimpleTypeInfo(Type::VARCHAR), 2),
            valueEncodeConfig: ValueEncodeConfig::default(),
        );
    }

    public function testAVectorThatFitsItsDeclaredCellLengthIsStillRead(): void {
        $body = pack('G', 1.0) . pack('G', 2.0);
        $stream = new StreamReader($body . str_repeat("\x00", 512));

        $vector = Vector::fromStream(
            $stream,
            length: strlen($body),
            typeInfo: new VectorInfo(new SimpleTypeInfo(Type::FLOAT), 2),
            valueEncodeConfig: ValueEncodeConfig::default(),
        );

        $this->assertSame([1.0, 2.0], $vector->getValue());
    }
}
