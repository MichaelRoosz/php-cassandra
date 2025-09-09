<?php

declare(strict_types=1);

namespace Cassandra;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueFactoryException;
use Cassandra\Response\StreamReader;
use Cassandra\Value as Values;
use Cassandra\TypeInfo\ListCollectionInfo;
use Cassandra\TypeInfo\MapCollectionInfo;
use Cassandra\TypeInfo\SetCollectionInfo;
use Cassandra\TypeInfo\CustomInfo;
use Cassandra\TypeInfo\SimpleTypeInfo;
use Cassandra\TypeInfo\TupleInfo;
use Cassandra\TypeInfo\TypeInfo;
use Cassandra\TypeInfo\UDTInfo;
use Cassandra\TypeInfo\VectorInfo;
use Cassandra\Value\ValueEncodeConfig;

final class ValueFactory {
    /**
     * @param mixed $value
     *
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ValueException
     */
    public static function getBinaryByTypeInfo(TypeInfo $typeInfo, mixed $value): string {
        $valueObject = self::getValueObjectFromValue($typeInfo, $value);
        if ($valueObject === null) {
            throw new ValueFactoryException('Cannot get type object for value', ExceptionCode::VALUEFACTORY_CANNOT_GET_TYPE_OBJECT_FOR_VALUE->value, [
                'value_type' => gettype($value),
                'target_type' => $typeInfo->type->name,
            ]);
        }

        return $valueObject->getBinary();
    }

    /**
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    public static function getSerializedLengthOfType(Type $type): int {
        $class = self::getClassForDataType($type);

        return $class::fixedLength();
    }

    /**
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    public static function getTypeInfoFromType(Type $type): TypeInfo {

        $typeClassMap = self::getTypeToValueClassMap();

        if (!isset($typeClassMap[$type->value])) {
            throw new ValueFactoryException('Unknown data type', ExceptionCode::VALUEFACTORY_UNKNOWN_DATA_TYPE->value, [
                'type' => $type->value,
                'type_name' => $type->name,
                'supported_types' => array_keys($typeClassMap),
            ]);
        }

        if (!self::isSimpleType($type)) {
            throw new ValueFactoryException('Cannot get type info from complex type without definition', ExceptionCode::VALUEFACTORY_COMPLEX_TYPEINFO_REQUIRED->value, [
                'type' => $type->value,
                'type_name' => $type->name,
                'context' => 'complex_types_need_definition',
            ]);
        }

        return new SimpleTypeInfo($type);
    }

    /**
     * @param array<mixed>|\Cassandra\Type $typeDefinition
     * 
     * @throws \Cassandra\Exception\TypeInfoException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ValueException
     */
    public static function getTypeInfoFromTypeDefinition(array|Type $typeDefinition): TypeInfo {

        if ($typeDefinition instanceof Type) {
            return self::getTypeInfoFromType($typeDefinition);
        }

        if (!isset($typeDefinition['type'])) {
            throw new ValueFactoryException('Type definition must have a type property', ExceptionCode::VALUEFACTORY_TYPEDEF_MISSING_TYPE->value, [
                'typeDefinition' => $typeDefinition,
            ]);
        }

        if (!($typeDefinition['type'] instanceof Type)) {
            throw new ValueFactoryException('Type property must be an instance of Type', ExceptionCode::VALUEFACTORY_TYPEDEF_TYPE_NOT_INSTANCE->value, [
                'typeDefinition' => $typeDefinition,
            ]);
        }

        $type = $typeDefinition['type'];

        /** @psalm-suppress InvalidArgument */
        return match ($type) {
            /** @phpstan-ignore argument.type */
            Type::CUSTOM => CustomInfo::fromTypeDefinition($typeDefinition),

            /** @phpstan-ignore argument.type */
            Type::LIST => ListCollectionInfo::fromTypeDefinition($typeDefinition),

            /** @phpstan-ignore argument.type */
            Type::SET => SetCollectionInfo::fromTypeDefinition($typeDefinition),

            /** @phpstan-ignore argument.type */
            Type::MAP => MapCollectionInfo::fromTypeDefinition($typeDefinition),

            /** @phpstan-ignore argument.type */
            Type::UDT => UDTInfo::fromTypeDefinition($typeDefinition),

            /** @phpstan-ignore argument.type */
            Type::TUPLE => TupleInfo::fromTypeDefinition($typeDefinition),

            /** @phpstan-ignore argument.type */
            Type::VECTOR => VectorInfo::fromTypeDefinition($typeDefinition),

            default => SimpleTypeInfo::fromTypeDefinition($typeDefinition),
        };
    }

    /**
    * @throws \Cassandra\Exception\ValueFactoryException
    * @throws \Cassandra\Exception\ValueException
    */
    public static function getValueObjectFromBinary(
        TypeInfo $typeInfo,
        string $binary,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): Values\ValueBase {

        $class = self::getClassForDataType($typeInfo->type);

        $valueObject = $class::fromBinary($binary, $typeInfo, $valueEncodeConfig);

        return $valueObject;
    }

    /**
    * @throws \Cassandra\Exception\ValueFactoryException
    * @throws \Cassandra\Exception\ValueException
    */
    public static function getValueObjectFromStream(
        TypeInfo $typeInfo,
        ?int $length,
        StreamReader $stream,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): Values\ValueBase {

        $class = self::getClassForDataType($typeInfo->type);

        $valueObject = $class::fromStream($stream, $length, $typeInfo, $valueEncodeConfig);

        return $valueObject;
    }

    /**
     * @param mixed $value
     *
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ValueException
     */
    public static function getValueObjectFromValue(TypeInfo $typeInfo, mixed $value): ?Values\ValueBase {
        if ($value === null) {
            return null;
        }

        $class = self::getClassForDataType($typeInfo->type);

        return $class::fromMixedValue($value, $typeInfo);
    }

    /**
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    public static function isSerializedAsFixedLength(Type $type): bool {
        $class = self::getClassForDataType($type);

        return $class::isSerializedAsFixedLength();
    }

    /**
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    public static function isSimpleType(Type $type): bool {
        $class = self::getClassForDataType($type);

        return !$class::requiresDefinition();
    }

    /**
     * @return class-string<\Cassandra\Value\ValueBase>
     *
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    protected static function getClassForDataType(Type $type): string {

        $typeClassMap = self::getTypeToValueClassMap();

        if (!isset($typeClassMap[$type->value])) {
            throw new ValueFactoryException('Unknown data type', ExceptionCode::VALUEFACTORY_CLASS_UNKNOWN_DATA_TYPE->value, [
                'type' => $type->value,
                'type_name' => $type->name,
                'supported_types' => array_keys($typeClassMap),
            ]);
        }

        return $typeClassMap[$type->value];
    }

    /**
     * @todo this should be moved to a const class value once support for php 8.1 is dropped
     * 
     * @return array<int, class-string<\Cassandra\Value\ValueBase>>
     */
    protected static function getTypeToValueClassMap(): array {
        return [
            Type::ASCII->value => Values\Ascii::class,
            Type::BIGINT->value => Values\Bigint::class,
            Type::BLOB->value => Values\Blob::class,
            Type::BOOLEAN->value => Values\Boolean::class,
            Type::COUNTER->value => Values\Counter::class,
            Type::CUSTOM->value => Values\Custom::class,
            Type::DATE->value => Values\Date::class,
            Type::DECIMAL->value => Values\Decimal::class,
            Type::DOUBLE->value => Values\Double::class,
            Type::DURATION->value => Values\Duration::class,
            Type::FLOAT->value => Values\Float32::class,
            Type::INET->value => Values\Inet::class,
            Type::INT->value => Values\Int32::class,
            Type::LIST->value => Values\ListCollection::class,
            Type::MAP->value => Values\MapCollection::class,
            Type::SET->value => Values\SetCollection::class,
            Type::SMALLINT->value => Values\Smallint::class,
            Type::TEXT->value => Values\Varchar::class,  // deprecated in protocol v3
            Type::TIME->value => Values\Time::class,
            Type::TIMESTAMP->value => Values\Timestamp::class,
            Type::TIMEUUID->value => Values\Timeuuid::class,
            Type::TINYINT->value => Values\Tinyint::class,
            Type::TUPLE->value => Values\Tuple::class,
            Type::UDT->value => Values\UDT::class,
            Type::UUID->value => Values\Uuid::class,
            Type::VARCHAR->value => Values\Varchar::class,
            Type::VARINT->value => Values\Varint::class,
            Type::VECTOR->value => Values\Vector::class,
        ];
    }
}
