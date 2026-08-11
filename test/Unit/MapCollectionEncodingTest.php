<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\Response\StreamReader;
use Cassandra\Type;
use Cassandra\TypeInfo\MapCollectionInfo;
use Cassandra\Value\EncodeOption\MapEncodeOption;
use Cassandra\Value\EncodeOption\TimestampEncodeOption;
use Cassandra\Value\MapCollection;
use Cassandra\Value\MapEntry;
use Cassandra\Value\ValueEncodeConfig;
use Cassandra\ValueFactory;
use DateTimeImmutable;

final class MapCollectionEncodingTest extends AbstractUnitTestCase {
    public function testAutoReturnsMapCollectionForConfiguredObjectKeys(): void {
        $typeInfo = $this->mapTypeInfo(Type::TIMESTAMP, Type::TIMESTAMP);
        $map = MapCollection::fromEntries(
            [new MapEntry(new DateTimeImmutable('@1'), new DateTimeImmutable('@2'))],
            Type::TIMESTAMP,
            Type::TIMESTAMP,
        );
        $config = new ValueEncodeConfig(
            timestampEncodeOption: TimestampEncodeOption::AS_DATETIME_IMMUTABLE,
        );

        $decoded = $this->decodeRowValue($map, $typeInfo, $config);

        $this->assertInstanceOf(MapCollection::class, $decoded);
        $this->assertCount(1, $decoded->getEntries());
        $this->assertInstanceOf(DateTimeImmutable::class, $decoded->getEntries()[0]->key);
        $this->assertInstanceOf(DateTimeImmutable::class, $decoded->getEntries()[0]->value);
        $this->assertSame($map->getBinary(), $decoded->getBinary());
    }
    public function testAutoReturnsNativeArrayForArraySafeKeys(): void {
        $typeInfo = $this->mapTypeInfo(Type::VARCHAR, Type::INT);
        $map = MapCollection::fromValue(['a' => 1, 'b' => 2], Type::VARCHAR, Type::INT);

        $this->assertSame(['a' => 1, 'b' => 2], $this->decodeRowValue($map, $typeInfo));
    }

    public function testAutoReturnsNativeArrayForArraySafeTemporalConfiguration(): void {
        $typeInfo = $this->mapTypeInfo(Type::TIMESTAMP, Type::INT);
        $map = MapCollection::fromEntries(
            [new MapEntry(new DateTimeImmutable('@1'), 2)],
            Type::TIMESTAMP,
            Type::INT,
        );

        $decoded = $this->decodeRowValue($map, $typeInfo);

        $this->assertIsArray($decoded);
        $this->assertIsString(array_key_first($decoded));
        $this->assertSame(2, reset($decoded));
    }

    public function testAutoUsesSchemaForEmptyComplexKeyMap(): void {
        $tupleDefinition = ['type' => Type::TUPLE, 'valueTypes' => [Type::INT]];
        $typeInfo = $this->mapTypeInfo($tupleDefinition, Type::INT);
        $map = MapCollection::fromEntries([], $tupleDefinition, Type::INT);

        $this->assertInstanceOf(MapCollection::class, $this->decodeRowValue($map, $typeInfo));
    }

    public function testForcedArrayRejectsConfiguredObjectKeysWithProjectException(): void {
        $typeInfo = $this->mapTypeInfo(Type::TIMESTAMP, Type::INT);
        $map = MapCollection::fromEntries(
            [new MapEntry(new DateTimeImmutable('@1'), 2)],
            Type::TIMESTAMP,
            Type::INT,
        );
        $config = new ValueEncodeConfig(
            timestampEncodeOption: TimestampEncodeOption::AS_DATETIME_IMMUTABLE,
            mapEncodeOption: MapEncodeOption::AS_ARRAY,
        );

        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_MAP_CANNOT_CONVERT_TO_ARRAY->value);

        $this->decodeRowValue($map, $typeInfo, $config);
    }

    public function testMapCollectionCanBeRequestedForScalarKeys(): void {
        $typeInfo = $this->mapTypeInfo(Type::VARCHAR, Type::INT);
        $map = MapCollection::fromValue(['a' => 1], Type::VARCHAR, Type::INT);
        $config = new ValueEncodeConfig(mapEncodeOption: MapEncodeOption::AS_MAP_COLLECTION);

        $decoded = $this->decodeRowValue($map, $typeInfo, $config);

        $this->assertInstanceOf(MapCollection::class, $decoded);
        $this->assertSame(['a' => 1], $decoded->getValue());
    }

    public function testTupleKeyMapUsesMapCollectionAndRoundTrips(): void {
        $tupleDefinition = [
            'type' => Type::TUPLE,
            'valueTypes' => [Type::INT, Type::VARCHAR],
        ];
        $typeInfo = $this->mapTypeInfo($tupleDefinition, Type::INT);
        $map = MapCollection::fromEntries(
            [new MapEntry([7, 'seven'], 8)],
            $tupleDefinition,
            Type::INT,
        );

        $decoded = $this->decodeRowValue($map, $typeInfo);

        $this->assertInstanceOf(MapCollection::class, $decoded);
        $this->assertSame([7, 'seven'], $decoded->getEntries()[0]->key);
        $this->assertSame(8, $decoded->getEntries()[0]->value);
        $this->assertSame($map->getBinary(), $decoded->getBinary());

        try {
            $decoded->getValue();
            $this->fail('A tuple-keyed map must not silently collapse into a PHP array');
        } catch (ValueException $e) {
            $this->assertSame(ExceptionCode::VALUE_MAP_CANNOT_CONVERT_TO_ARRAY->value, $e->getCode());
        }
    }

    private function decodeRowValue(
        MapCollection $map,
        MapCollectionInfo $typeInfo,
        ?ValueEncodeConfig $config = null,
    ): mixed {
        $binary = $map->getBinary();

        return (new StreamReader(pack('N', strlen($binary)) . $binary))->readValue(
            $typeInfo,
            $config ?? ValueEncodeConfig::default(),
        );
    }

    /**
     * @param Type|(array{type: Type}&array<mixed>) $keyDefinition
     * @param Type|(array{type: Type}&array<mixed>) $valueDefinition
     */
    private function mapTypeInfo(Type|array $keyDefinition, Type|array $valueDefinition): MapCollectionInfo {
        $typeInfo = ValueFactory::getTypeInfoFromTypeDefinition([
            'type' => Type::MAP,
            'keyType' => $keyDefinition,
            'valueType' => $valueDefinition,
        ]);

        if (!$typeInfo instanceof MapCollectionInfo) {
            $this->fail('Expected map type info');
        }

        return $typeInfo;
    }
}
