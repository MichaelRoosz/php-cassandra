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

final class TypeFactory {
    protected const TYPE_CLASS_MAP = [
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
    ];

    protected const TYPES_WITH_DEFINITION = [
        Type::COLLECTION_LIST->value => true,
        Type::COLLECTION_SET->value => true,
        Type::COLLECTION_MAP->value => true,
        Type::UDT->value => true,
        Type::TUPLE->value => true,
        Type::CUSTOM->value => true,
    ];

    /**
     * @param mixed $value
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function getBinaryByTypeInfo(TypeInfo $typeInfo, mixed $value): string {
        $type = self::getTypeObjectForValue($typeInfo, $value);
        if ($type === null) {
            throw new Exception('Cannot get type object for value', Exception::CODE_TYPEFACTORY_CANNOT_GET_TYPE_OBJECT_FOR_VALUE, [
                'value_type' => gettype($value),
                'type_info' => get_class($typeInfo),
            ]);
        }

        return $type->getBinary();
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public static function getTypeInfoFromType(Type $type): TypeInfo {
        if (!isset(self::TYPE_CLASS_MAP[$type->value])) {
            throw new Exception('Unknown data type', Exception::CODE_TYPEFACTORY_UNKNOWN_DATA_TYPE, [
                'type' => $type->value,
                'type_name' => $type->name,
                'supported_types' => array_keys(self::TYPE_CLASS_MAP),
            ]);
        }

        if (!self::isSimpleType($type)) {
            throw new Exception('Cannot get type info from complex type without definition', Exception::CODE_TYPEFACTORY_COMPLEX_TYPEINFO_REQUIRED, [
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
            throw new Exception('Type definition must have a type property', Exception::CODE_TYPEFACTORY_TYPEDEF_MISSING_TYPE, [
                'typeDefinition' => $typeDefinition,
            ]);
        }

        if (!($typeDefinition['type'] instanceof Type)) {
            throw new Exception('Type property must be an instance of Type', Exception::CODE_TYPEFACTORY_TYPEDEF_TYPE_NOT_INSTANCE, [
                'typeDefinition' => $typeDefinition,
            ]);
        }

        $type = $typeDefinition['type'];

        switch ($type) {
            case Type::CUSTOM:
                /** @psalm-suppress InvalidArgument */
                /** @phpstan-ignore argument.type */
                return CustomInfo::fromTypeDefinition($typeDefinition);

            case Type::COLLECTION_LIST:
                /** @psalm-suppress InvalidArgument */
                /** @phpstan-ignore argument.type */
                return CollectionListInfo::fromTypeDefinition($typeDefinition);

            case Type::COLLECTION_SET:
                /** @psalm-suppress InvalidArgument */
                /** @phpstan-ignore argument.type */
                return CollectionSetInfo::fromTypeDefinition($typeDefinition);

            case Type::COLLECTION_MAP:
                /** @psalm-suppress InvalidArgument */
                /** @phpstan-ignore argument.type */
                return CollectionMapInfo::fromTypeDefinition($typeDefinition);

            case Type::UDT:
                /** @psalm-suppress InvalidArgument */
                /** @phpstan-ignore argument.type */
                return UDTInfo::fromTypeDefinition($typeDefinition);

            case Type::TUPLE:
                /** @psalm-suppress InvalidArgument */
                /** @phpstan-ignore argument.type */
                return TupleInfo::fromTypeDefinition($typeDefinition);

            default:
                /** @psalm-suppress InvalidArgument */
                return SimpleTypeInfo::fromTypeDefinition($typeDefinition);
        }
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

    public static function isSimpleType(Type $type): bool {
        return !isset(self::TYPES_WITH_DEFINITION[$type->value]);
    }

    /**
     * @return class-string<\Cassandra\Type\TypeBase>
     * 
     * @throws \Cassandra\Type\Exception
     */
    protected static function getClassForDataType(Type $type): string {

        if (!isset(self::TYPE_CLASS_MAP[$type->value])) {
            throw new Exception('Unknown data type', Exception::CODE_TYPEFACTORY_CLASS_UNKNOWN_DATA_TYPE, [
                'type' => $type->value,
                'type_name' => $type->name,
                'supported_types' => array_keys(self::TYPE_CLASS_MAP),
            ]);
        }

        return self::TYPE_CLASS_MAP[$type->value];
    }
}
