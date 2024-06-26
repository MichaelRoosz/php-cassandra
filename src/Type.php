<?php

declare(strict_types=1);

namespace Cassandra;

use Cassandra\Type\Exception;
use Cassandra\Type as Types;

class Type {
    final public const ASCII = 0x0001;
    final public const BIGINT = 0x0002;
    final public const BLOB = 0x0003;
    final public const BOOLEAN = 0x0004;
    final public const COLLECTION_LIST = 0x0020;
    final public const COLLECTION_MAP = 0x0021;
    final public const COLLECTION_SET = 0x0022;
    final public const COUNTER = 0x0005;
    final public const CUSTOM = 0x0000;
    final public const DATE = 0x0011;
    final public const DECIMAL = 0x0006;
    final public const DOUBLE = 0x0007;
    final public const DURATION = 0x0015;
    final public const FLOAT = 0x0008;
    final public const INET = 0x0010;
    final public const INT = 0x0009;
    final public const SMALLINT = 0x0013;
    final public const TEXT = 0x000A;        // deprecated in Protocol v3
    final public const TIME = 0x0012;
    final public const TIMESTAMP = 0x000B;
    final public const TIMEUUID = 0x000F;
    final public const TINYINT = 0x0014;
    final public const TUPLE = 0x0031;
    final public const UDT = 0x0030;
    final public const UUID = 0x000C;
    final public const VARCHAR = 0x000D;
    final public const VARINT = 0x000E;

    /**
     * @var array<int, class-string<Types\TypeBase>> $typeClassMap
     */
    protected static array $typeClassMap = [
        self::ASCII     => Types\Ascii::class,
        self::VARCHAR   => Types\Varchar::class,
        self::TEXT      => Types\Varchar::class,  // deprecated in Protocol v3
        self::VARINT    => Types\Varint::class,
        self::BIGINT    => Types\Bigint::class,
        self::COUNTER   => Types\Counter::class,
        self::TIMESTAMP => Types\Timestamp::class,
        self::BLOB      => Types\Blob::class,
        self::BOOLEAN   => Types\Boolean::class,
        self::DECIMAL   => Types\Decimal::class,
        self::DOUBLE    => Types\Double::class,
        self::FLOAT     => Types\Float32::class,
        self::INT       => Types\Integer::class,
        self::UUID      => Types\Uuid::class,
        self::TIMEUUID  => Types\Timeuuid::class,
        self::INET      => Types\Inet::class,
        self::DATE      => Types\Date::class,
        self::TIME      => Types\Time::class,
        self::SMALLINT  => Types\Smallint::class,
        self::TINYINT   => Types\Tinyint::class,
        self::DURATION  => Types\Duration::class,
        self::COLLECTION_LIST => Types\CollectionList::class,
        self::COLLECTION_SET  => Types\CollectionSet::class,
        self::COLLECTION_MAP  => Types\CollectionMap::class,
        self::UDT       => Types\UDT::class,
        self::TUPLE     => Types\Tuple::class,
        self::CUSTOM    => Types\Custom::class,
    ];

    /**
     * @var array<int> $typesWithDefinition
     */
    protected static array $typesWithDefinition = [
        self::COLLECTION_LIST,
        self::COLLECTION_SET,
        self::COLLECTION_MAP,
        self::UDT,
        self::TUPLE,
        self::CUSTOM,
    ];

    protected function __construct() {
    }

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
     * @deprecated Use getTypeObjectForValue() instead
     * @param int|array<mixed> $dataType
     * @param mixed $value
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function getTypeObject(int|array $dataType, mixed $value): ?Types\TypeBase {
        return self::getTypeObjectForValue($dataType, $value);
    }

    /**
    * @param int|array<mixed> $dataType
    *
    * @throws \Cassandra\Type\Exception
    */
    public static function getTypeObjectForBinary(int|array $dataType, string $binary): Types\TypeBase {
        $dataTypeInfo = self::getTypeAndDefinitionOfDataType($dataType);

        if ($dataTypeInfo['type'] === self::CUSTOM) {
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

        if ($dataTypeInfo['type'] === self::CUSTOM) {
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
