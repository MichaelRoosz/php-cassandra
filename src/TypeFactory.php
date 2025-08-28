<?php

declare(strict_types=1);

namespace Cassandra;

use Cassandra\Type\Exception;
use Cassandra\Type as Types;
use Cassandra\TypeInfo\CollectionListInfo;
use Cassandra\TypeInfo\CollectionMapInfo;
use Cassandra\TypeInfo\CollectionSetInfo;
use Cassandra\TypeInfo\CustomInfo;
use Cassandra\TypeInfo\SimpleTypeInfo;
use Cassandra\TypeInfo\TupleInfo;
use Cassandra\TypeInfo\TypeInfo;
use Cassandra\TypeInfo\UDTInfo;
use Cassandra\TypeInfo\VectorInfo;

final class TypeFactory {
    /**
     * @param mixed $value
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function getBinaryByTypeInfo(TypeInfo $typeInfo, mixed $value): string {
        $type = self::getTypeObjectForValue($typeInfo, $value);
        if ($type === null) {
            throw new Exception('Cannot get type object for value', ExceptionCode::TYPE_FACTORY_CANNOT_GET_TYPE_OBJECT_FOR_VALUE->value, [
                'value_type' => gettype($value),
                'type_info' => get_class($typeInfo),
            ]);
        }

        return $type->getBinary();
    }

    public static function getSerializedSizeOfType(Type $type): int {
        $map = self::getTypesSerializedAsFixedSize();

        return $map[$type->value] ?? -1;
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public static function getTypeInfoFromType(Type $type): TypeInfo {

        $typeClassMap = self::getTypeClassMap();

        if (!isset($typeClassMap[$type->value])) {
            throw new Exception('Unknown data type', ExceptionCode::TYPE_FACTORY_UNKNOWN_DATA_TYPE->value, [
                'type' => $type->value,
                'type_name' => $type->name,
                'supported_types' => array_keys($typeClassMap),
            ]);
        }

        if (!self::isSimpleType($type)) {
            throw new Exception('Cannot get type info from complex type without definition', ExceptionCode::TYPE_FACTORY_COMPLEX_TYPEINFO_REQUIRED->value, [
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
     * @throws \Cassandra\TypeInfo\Exception
     * @throws \Cassandra\Type\Exception
     */
    public static function getTypeInfoFromTypeDefinition(array|Type $typeDefinition): TypeInfo {

        if ($typeDefinition instanceof Type) {
            return self::getTypeInfoFromType($typeDefinition);
        }

        if (!isset($typeDefinition['type'])) {
            throw new Exception('Type definition must have a type property', ExceptionCode::TYPE_FACTORY_TYPEDEF_MISSING_TYPE->value, [
                'typeDefinition' => $typeDefinition,
            ]);
        }

        if (!($typeDefinition['type'] instanceof Type)) {
            throw new Exception('Type property must be an instance of Type', ExceptionCode::TYPE_FACTORY_TYPEDEF_TYPE_NOT_INSTANCE->value, [
                'typeDefinition' => $typeDefinition,
            ]);
        }

        $type = $typeDefinition['type'];

        /** @psalm-suppress InvalidArgument */
        return match ($type) {
            /** @phpstan-ignore argument.type */
            Type::CUSTOM => CustomInfo::fromTypeDefinition($typeDefinition),

            /** @phpstan-ignore argument.type */
            Type::COLLECTION_LIST => CollectionListInfo::fromTypeDefinition($typeDefinition),

            /** @phpstan-ignore argument.type */
            Type::COLLECTION_SET => CollectionSetInfo::fromTypeDefinition($typeDefinition),

            /** @phpstan-ignore argument.type */
            Type::COLLECTION_MAP => CollectionMapInfo::fromTypeDefinition($typeDefinition),

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
    * @throws \Cassandra\Type\Exception
    */
    public static function getTypeObjectForBinary(TypeInfo $typeInfo, string $binary): Types\TypeBase {

        $class = self::getClassForDataType($typeInfo->type);

        return $class::fromBinary($binary, $typeInfo);
    }

    /**
     * @param mixed $value
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function getTypeObjectForValue(TypeInfo $typeInfo, mixed $value): ?Types\TypeBase {
        if ($value === null) {
            return null;
        }

        $class = self::getClassForDataType($typeInfo->type);

        return $class::fromMixedValue($value, $typeInfo);
    }

    public static function isSerializedAsFixedSize(Type $type): bool {
        $map = self::getTypesSerializedAsFixedSize();

        return isset($map[$type->value]);
    }

    public static function isSimpleType(Type $type): bool {
        $typesWithDefinition = self::getTypesWithDefinitionList();

        return !isset($typesWithDefinition[$type->value]);
    }

    /**
     * @return class-string<\Cassandra\Type\TypeBase>
     * 
     * @throws \Cassandra\Type\Exception
     */
    protected static function getClassForDataType(Type $type): string {

        $typeClassMap = self::getTypeClassMap();

        if (!isset($typeClassMap[$type->value])) {
            throw new Exception('Unknown data type', ExceptionCode::TYPE_FACTORY_CLASS_UNKNOWN_DATA_TYPE->value, [
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
     * @return array<int, class-string<\Cassandra\Type\TypeBase>>
     */
    protected static function getTypeClassMap(): array {
        return [
            Type::ASCII->value => Types\Ascii::class,
            Type::VARCHAR->value => Types\Varchar::class,
            Type::TEXT->value => Types\Varchar::class,  // deprecated in Protocol v3
            Type::VARINT->value => Types\Varint::class,
            Type::BIGINT->value => Types\Bigint::class,
            Type::COUNTER->value => Types\Counter::class,
            Type::TIMESTAMP->value => Types\Timestamp::class,
            Type::BLOB->value => Types\Blob::class,
            Type::BOOLEAN->value => Types\Boolean::class,
            Type::DECIMAL->value => Types\Decimal::class,
            Type::DOUBLE->value => Types\Double::class,
            Type::FLOAT->value => Types\Float32::class,
            Type::INT->value => Types\Integer::class,
            Type::UUID->value => Types\Uuid::class,
            Type::TIMEUUID->value => Types\Timeuuid::class,
            Type::INET->value => Types\Inet::class,
            Type::DATE->value => Types\Date::class,
            Type::TIME->value => Types\Time::class,
            Type::SMALLINT->value => Types\Smallint::class,
            Type::TINYINT->value => Types\Tinyint::class,
            Type::DURATION->value => Types\Duration::class,
            Type::COLLECTION_LIST->value => Types\CollectionList::class,
            Type::COLLECTION_SET->value => Types\CollectionSet::class,
            Type::COLLECTION_MAP->value => Types\CollectionMap::class,
            Type::UDT->value => Types\UDT::class,
            Type::TUPLE->value => Types\Tuple::class,
            Type::CUSTOM->value => Types\Custom::class,
            Type::VECTOR->value => Types\Vector::class,
        ];
    }

    /**
     * @todo this should be moved to a const class value once support for php 8.1 is dropped
     * 
     * @return array<int, int>
     */
    protected static function getTypesSerializedAsFixedSize(): array {
        return [
            Type::BIGINT->value => 8,
            Type::BOOLEAN->value => 1,
            Type::COUNTER->value => 8,
            Type::DATE->value => 8,
            Type::DOUBLE->value => 8,
            Type::FLOAT->value => 4,
            Type::INT->value => 4,
            Type::TIME->value => 8,
            Type::TIMESTAMP->value => 8,
            Type::TIMEUUID->value => 16,
            Type::UUID->value => 16,

            // note: logically smallint and tinyint are fixed size,
            // but in cassandra they are defined as variable size
        ];
    }

    /**
     * @todo this should be moved to a const class value once support for php 8.1 is dropped
     * 
     * @return array<int, bool>
     */
    protected static function getTypesWithDefinitionList(): array {
        return [
            Type::COLLECTION_LIST->value => true,
            Type::COLLECTION_SET->value => true,
            Type::COLLECTION_MAP->value => true,
            Type::UDT->value => true,
            Type::TUPLE->value => true,
            Type::CUSTOM->value => true,
            Type::VECTOR->value => true,
        ];
    }
}
