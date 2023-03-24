<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Stringable;

abstract class Base implements Stringable {
    public const CUSTOM = 0x0000;
    public const ASCII = 0x0001;
    public const BIGINT = 0x0002;
    public const BLOB = 0x0003;
    public const BOOLEAN = 0x0004;
    public const COUNTER = 0x0005;
    public const DECIMAL = 0x0006;
    public const DOUBLE = 0x0007;
    public const FLOAT = 0x0008;
    public const INT = 0x0009;
    public const TEXT = 0x000A;        // deprecated in Protocol v3
    public const TIMESTAMP = 0x000B;
    public const UUID = 0x000C;
    public const VARCHAR = 0x000D;
    public const VARINT = 0x000E;
    public const TIMEUUID = 0x000F;
    public const INET = 0x0010;
    public const DATE = 0x0011;
    public const TIME = 0x0012;
    public const SMALLINT = 0x0013;
    public const TINYINT = 0x0014;
    public const DURATION = 0x0015;

    public const COLLECTION_LIST = 0x0020;
    public const COLLECTION_MAP = 0x0021;
    public const COLLECTION_SET = 0x0022;
    public const UDT = 0x0030;
    public const TUPLE = 0x0031;

    /**
     * @var array<int, class-string<Base>> $typeClassMap
     */
    public static array $typeClassMap = [
        self::ASCII     => Ascii::class,
        self::VARCHAR   => Varchar::class,
        self::TEXT      => Varchar::class,  // deprecated in Protocol v3
        self::VARINT    => Varint::class,
        self::BIGINT    => Bigint::class,
        self::COUNTER   => Counter::class,
        self::TIMESTAMP => Timestamp::class,
        self::BLOB      => Blob::class,
        self::BOOLEAN   => Boolean::class,
        self::DECIMAL   => Decimal::class,
        self::DOUBLE    => Double::class,
        self::FLOAT     => PhpFloat::class,
        self::INT       => PhpInt::class,
        self::UUID      => Uuid::class,
        self::TIMEUUID  => Timeuuid::class,
        self::INET      => Inet::class,
        self::DATE      => Date::class,
        self::TIME      => Time::class,
        self::SMALLINT  => Smallint::class,
        self::TINYINT   => Tinyint::class,
        self::DURATION  => Duration::class,
        self::COLLECTION_LIST => CollectionList::class,
        self::COLLECTION_SET  => CollectionSet::class,
        self::COLLECTION_MAP  => CollectionMap::class,
        self::UDT       => UDT::class,
        self::TUPLE     => Tuple::class,
        self::CUSTOM    => Custom::class,
    ];

    /**
     * @var array<int> $typesWithDefinition
     */
    public static array $typesWithDefinition = [
        self::COLLECTION_LIST,
        self::COLLECTION_SET,
        self::COLLECTION_MAP,
        self::UDT,
        self::TUPLE,
        self::CUSTOM,
    ];

    protected ?string $_binary = null;

    abstract public function __toString(): string;

    /**
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    abstract public static function parse(string $binary, null|int|array $definition = null): mixed;

    public function setBinary(string $binary): static {
        $this->_binary = $binary;
        $this->resetValue();

        return $this;
    }

    public function getBinary(): string {
        if ($this->_binary === null) {
            $this->_binary = $this->binaryOfValue();
        }

        return $this->_binary;
    }

    public function getValue(): mixed {
        return $this->parseValue();
    }

    /**
     * @param int|array<mixed> $dataType
     * @param mixed $value
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function getBinaryByType(int|array $dataType, mixed $value): string {
        $type = static::getTypeObject($dataType, $value);
        if ($type === null) {
            throw new Exception('Cannot get type object');
        }

        return $type->binaryOfValue();
    }

    /**
     * @param int|array<mixed> $dataType
     * @param mixed $value
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function getTypeObject(int|array $dataType, mixed $value): ?Base {
        if ($value === null) {
            return null;
        }

        if (is_int($dataType)) {
            $type = $dataType;
            $definition = null;
        } else {
            $didShift = false;

            if (isset($dataType['type'])) {
                /** @var int|array<mixed> $type */
                $type = $dataType['type'];
            } else {
                /** @var int|array<mixed> $type */
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

        if ($definition === null && in_array($type, self::$typesWithDefinition)) {
            throw new Exception('type is missing its definition');
        }

        if (!is_int($type)) {
            throw new Exception('invalid data type');
        }

        if ($type === self::CUSTOM) {
            if (!is_string($value)) {
                throw new Exception('custom value is not a string');
            }

            if (!isset($dataType['definition'][0]) || !is_array($dataType['definition'])) {
                throw new Exception('cannot read custom java class name');
            }

            $javaClassName = $dataType['definition'][0];

            if (!is_string($javaClassName)) {
                throw new Exception('custom java class name is not a string');
            }

            return new Custom($value, $javaClassName);
        }

        if (!isset(self::$typeClassMap[$type])) {
            throw new Exception('unknown data type');
        }

        $class = self::$typeClassMap[$type];

        if (!is_subclass_of($class, Base::class)) {
            throw new Exception('data type is not a subclass of \\Cassandra\\Type\\Base');
        }

        return $class::create($value, $definition);
    }

    abstract protected function binaryOfValue(): string;

    abstract protected function parseValue(): mixed;

    abstract protected function resetValue(): void;

    /**
     * @param mixed $value
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    abstract protected static function create(mixed $value, null|int|array $definition): Base;
}
