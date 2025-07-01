<?php

declare(strict_types=1);

namespace Cassandra;

use Cassandra\Type\Exception;
use Cassandra\Type as Types;

final class TypeFactory {
    /**
     * @var array<int, class-string<Types\TypeBase>> $typeClassMap
     */
    protected static array $typeClassMap = [
        Type::ASCII->value     => Types\Ascii::class,
        Type::VARCHAR->value   => Types\Varchar::class,
        Type::TEXT->value      => Types\Varchar::class,  // deprecated in Protocol v3
        Type::VARINT->value    => Types\Varint::class,
        Type::BIGINT->value    => Types\Bigint::class,
        Type::COUNTER->value   => Types\Counter::class,
        Type::TIMESTAMP->value => Types\Timestamp::class,
        Type::BLOB->value      => Types\Blob::class,
        Type::BOOLEAN->value   => Types\Boolean::class,
        Type::DECIMAL->value   => Types\Decimal::class,
        Type::DOUBLE->value    => Types\Double::class,
        Type::FLOAT->value     => Types\Float32::class,
        Type::INT->value       => Types\Integer::class,
        Type::UUID->value      => Types\Uuid::class,
        Type::TIMEUUID->value  => Types\Timeuuid::class,
        Type::INET->value      => Types\Inet::class,
        Type::DATE->value      => Types\Date::class,
        Type::TIME->value      => Types\Time::class,
        Type::SMALLINT->value  => Types\Smallint::class,
        Type::TINYINT->value   => Types\Tinyint::class,
        Type::DURATION->value  => Types\Duration::class,
        Type::COLLECTION_LIST->value => Types\CollectionList::class,
        Type::COLLECTION_SET->value  => Types\CollectionSet::class,
        Type::COLLECTION_MAP->value  => Types\CollectionMap::class,
        Type::UDT->value       => Types\UDT::class,
        Type::TUPLE->value     => Types\Tuple::class,
        Type::CUSTOM->value    => Types\Custom::class,
    ];

    /**
     * @var array<int> $typesWithDefinition
     */
    protected static array $typesWithDefinition = [
        Type::COLLECTION_LIST->value,
        Type::COLLECTION_SET->value,
        Type::COLLECTION_MAP->value,
        Type::UDT->value,
        Type::TUPLE->value,
        Type::CUSTOM->value,
    ];

    /**
     * @param int|array<mixed> $dataType
     * @param mixed $value
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function getBinaryByType(int|array $dataType, mixed $value): string {
        $type = self::getTypeObjectForValue($dataType, $value);
        if ($type === null) {
            throw new Exception('Cannot get type object');
        }

        return $type->getBinary();
    }

    /**
    * @param int|array<mixed> $dataType
    *
    * @throws \Cassandra\Type\Exception
    */
    public static function getTypeObjectForBinary(int|array $dataType, string $binary): Types\TypeBase {
        $dataTypeInfo = self::getTypeAndDefinitionOfDataType($dataType);

        if ($dataTypeInfo['type'] === Type::CUSTOM->value) {
            if (!isset($dataType['definition']) || !is_array($dataType['definition']) || !isset($dataType['definition'][0])) {
                throw new Exception('cannot read custom java class name');
            }

            $javaClassName = $dataType['definition'][0];

            if (!is_string($javaClassName)) {
                throw new Exception('custom java class name is not a string');
            }

            return Types\Custom::fromBinary($binary, $dataTypeInfo['definition'], $javaClassName);
        }

        $class = self::getClassForDataType($dataTypeInfo['type'], $dataTypeInfo['definition']);

        return $class::fromBinary($binary, $dataTypeInfo['definition']);
    }

    /**
     * @param int|array<mixed> $dataType
     * @param mixed $value
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function getTypeObjectForValue(int|array $dataType, mixed $value): ?Types\TypeBase {
        if ($value === null) {
            return null;
        }

        $dataTypeInfo = self::getTypeAndDefinitionOfDataType($dataType);

        if ($dataTypeInfo['type'] === Type::CUSTOM->value) {
            if (!is_string($value)) {
                throw new Exception('custom value is not a string');
            }

            if (!isset($dataType['definition']) || !is_array($dataType['definition']) || !isset($dataType['definition'][0])) {
                throw new Exception('cannot read custom java class name');
            }

            $javaClassName = $dataType['definition'][0];

            if (!is_string($javaClassName)) {
                throw new Exception('custom java class name is not a string');
            }

            return new Types\Custom($value, $javaClassName);
        }

        $class = self::getClassForDataType($dataTypeInfo['type'], $dataTypeInfo['definition']);

        return $class::fromValue($value, $dataTypeInfo['definition']);
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     * @return class-string<Types\TypeBase>
     * 
     * @throws \Cassandra\Type\Exception
     */
    protected static function getClassForDataType(int $type, null|int|array $definition): string {
        if ($definition === null && in_array($type, self::$typesWithDefinition)) {
            throw new Exception('type is missing its definition');
        }

        if (!isset(self::$typeClassMap[$type])) {
            throw new Exception('unknown data type');
        }

        $class = self::$typeClassMap[$type];

        if (!is_subclass_of($class, Types\TypeBase::class)) {
            throw new Exception('data type is not a subclass of \\Cassandra\\Type\\TypeBase');
        }

        return $class;
    }

    /**
     * @param int|array<mixed> $dataType
     * @return array{
     *   type: int,
     *   definition: null|int|array<int|array<mixed>>,
     * }
     * 
     * @throws \Cassandra\Type\Exception
     */
    protected static function getTypeAndDefinitionOfDataType(int|array $dataType): array {
        if (is_int($dataType)) {
            $type = $dataType;
            $definition = null;
        } else {
            $didShift = false;

            if (isset($dataType['type'])) {
                /** @var mixed $type */
                $type = $dataType['type'];
            } else {
                /** @var mixed $type */
                $type = array_shift($dataType);
                $didShift = true;
            }

            if (isset($dataType['definition'])) {
                /** @var int|array<int|array<mixed>> $definition */
                $definition = $dataType['definition'];
            } elseif (isset($dataType['value'])) {
                /** @var int|array<int|array<mixed>> $definition */
                $definition = $dataType['value'];
            } elseif (isset($dataType['typeMap'])) {
                /** @var int|array<int|array<mixed>> $definition */
                $definition = $dataType['typeMap'];
            } else {
                if (!$didShift) {
                    array_shift($dataType);
                }

                /** @var int|array<int|array<mixed>> $definition */
                $definition = array_shift($dataType);
            }
        }

        if (!is_int($type)) {
            throw new Exception('invalid data type');
        }

        return [
            'type' => $type,
            'definition' => $definition,
        ];
    }
}
