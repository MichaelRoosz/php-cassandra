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

    protected function __construct() {
    }

    /**
     * @param int|array<mixed> $Type
     * @param mixed $value
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function getBinaryByType(int|array $Type, mixed $value): string {
        $type = self::getTypeObjectForValue($Type, $value);
        if ($type === null) {
            throw new Exception('Cannot get type object');
        }

        return $type->getBinary();
    }

    /**
     * @deprecated Use getTypeObjectForValue() instead
     * @param int|array<mixed> $Type
     * @param mixed $value
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function getTypeObject(int|array $Type, mixed $value): ?Types\TypeBase {
        return self::getTypeObjectForValue($Type, $value);
    }

    /**
    * @param int|array<mixed> $Type
    *
    * @throws \Cassandra\Type\Exception
    */
    public static function getTypeObjectForBinary(int|array $Type, string $binary): Types\TypeBase {
        $TypeInfo = self::getTypeAndDefinitionOfType($Type);

        if ($TypeInfo['type'] === Type::CUSTOM->value) {
            if (!isset($Type['definition']) || !is_array($Type['definition']) || !isset($Type['definition'][0])) {
                throw new Exception('cannot read custom java class name');
            }

            $javaClassName = $Type['definition'][0];

            if (!is_string($javaClassName)) {
                throw new Exception('custom java class name is not a string');
            }

            return Types\Custom::fromBinary($binary, $TypeInfo['definition'], $javaClassName);
        }

        $class = self::getClassForType($TypeInfo['type'], $TypeInfo['definition']);

        return $class::fromBinary($binary, $TypeInfo['definition']);
    }

    /**
     * @param int|array<mixed> $Type
     * @param mixed $value
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function getTypeObjectForValue(int|array $Type, mixed $value): ?Types\TypeBase {
        if ($value === null) {
            return null;
        }

        $TypeInfo = self::getTypeAndDefinitionOfType($Type);

        if ($TypeInfo['type'] === Type::CUSTOM->value) {
            if (!is_string($value)) {
                throw new Exception('custom value is not a string');
            }

            if (!isset($Type['definition']) || !is_array($Type['definition']) || !isset($Type['definition'][0])) {
                throw new Exception('cannot read custom java class name');
            }

            $javaClassName = $Type['definition'][0];

            if (!is_string($javaClassName)) {
                throw new Exception('custom java class name is not a string');
            }

            return new Types\Custom($value, $javaClassName);
        }

        $class = self::getClassForType($TypeInfo['type'], $TypeInfo['definition']);

        return $class::fromValue($value, $TypeInfo['definition']);
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     * @return class-string<Types\TypeBase>
     * 
     * @throws \Cassandra\Type\Exception
     */
    protected static function getClassForType(int $type, null|int|array $definition): string {
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
     * @param int|array<mixed> $Type
     * @return array{
     *   type: int,
     *   definition: null|int|array<int|array<mixed>>,
     * }
     * 
     * @throws \Cassandra\Type\Exception
     */
    protected static function getTypeAndDefinitionOfType(int|array $Type): array {
        if (is_int($Type)) {
            $type = $Type;
            $definition = null;
        } else {
            $didShift = false;

            if (isset($Type['type'])) {
                /** @var mixed $type */
                $type = $Type['type'];
            } else {
                /** @var mixed $type */
                $type = array_shift($Type);
                $didShift = true;
            }

            if (isset($Type['definition'])) {
                /** @var int|array<int|array<mixed>> $definition */
                $definition = $Type['definition'];
            } elseif (isset($Type['value'])) {
                /** @var int|array<int|array<mixed>> $definition */
                $definition = $Type['value'];
            } elseif (isset($Type['typeMap'])) {
                /** @var int|array<int|array<mixed>> $definition */
                $definition = $Type['typeMap'];
            } else {
                if (!$didShift) {
                    array_shift($Type);
                }

                /** @var int|array<int|array<mixed>> $definition */
                $definition = array_shift($Type);
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
