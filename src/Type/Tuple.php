<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\ExceptionCode;
use Cassandra\TypeFactory;
use Cassandra\Response\StreamReader;
use Cassandra\Type;
use Cassandra\TypeInfo\TupleInfo;
use Cassandra\TypeInfo\TypeInfo;

final class Tuple extends TypeReadableWithoutLength {
    protected TupleInfo $typeInfo;
    /**
     * @var array<mixed> $value
     */
    protected readonly array $value;

    /**
     * @param array<mixed> $value
     * @param list<\Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>)>|null $valueDefinition
     *
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\TypeInfo\Exception
     */
    final public function __construct(array $value, array|null $valueDefinition = null, ?TupleInfo $typeInfo = null) {
        $this->value = $value;

        if ($valueDefinition !== null) {
            $this->typeInfo = TupleInfo::fromTypeDefinition([
                'type' => Type::TUPLE,
                'valueTypes' => $valueDefinition,
            ]);
        } elseif ($typeInfo !== null) {
            $this->typeInfo = $typeInfo;
        } else {
            throw new Exception('Either valueDefinition or typeInfo must be provided', ExceptionCode::TYPE_TUPLE_VALUEDEF_OR_TYPEINFO_REQUIRED->value);
        }
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\TypeInfo\Exception
     */
    #[\Override]
    public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static {

        return self::fromStream(new StreamReader($binary), typeInfo: $typeInfo);
    }

    /**
     * @param mixed $value
     * 
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\TypeInfo\Exception
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        if (!is_array($value)) {
            throw new Exception('Invalid tuple value; expected array', ExceptionCode::TYPE_TUPLE_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        if ($typeInfo === null) {
            throw new Exception('typeInfo is required', ExceptionCode::TYPE_TUPLE_TYPEINFO_REQUIRED->value);
        }

        if (!$typeInfo instanceof TupleInfo) {
            throw new Exception('Invalid type info, TupleInfo expected', ExceptionCode::TYPE_TUPLE_INVALID_TYPEINFO->value, [
                'given_type' => get_class($typeInfo),
            ]);
        }

        return new static($value, typeInfo: $typeInfo);
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\TypeInfo\Exception
     */
    #[\Override]
    final public static function fromStream(StreamReader $stream, ?int $length = null, ?TypeInfo $typeInfo = null): static {
        if ($typeInfo === null) {
            throw new Exception('typeInfo is required', ExceptionCode::TYPE_TUPLE_TYPEINFO_REQUIRED->value);
        }

        if (!$typeInfo instanceof TupleInfo) {
            throw new Exception('Invalid type info, TupleInfo expected', ExceptionCode::TYPE_TUPLE_INVALID_TYPEINFO->value, [
                'given_type' => get_class($typeInfo),
            ]);
        }

        $tuple = [];
        foreach ($typeInfo->valueTypes as $key => $type) {
            /** @psalm-suppress MixedAssignment */
            $tuple[$key] = $stream->readValue($type);
        }

        return new static($tuple, typeInfo: $typeInfo);
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public function getBinary(): string {
        $binary = '';
        $value = $this->value;

        foreach ($this->typeInfo->valueTypes as $key => $type) {
            if ($value[$key] === null) {
                $binary .= "\xff\xff\xff\xff";
            } else {
                $valueBinary = $value[$key] instanceof TypeBase
                    ? $value[$key]->getBinary()
                    : TypeFactory::getBinaryByTypeInfo($type, $value[$key]);

                $binary .= pack('N', strlen($valueBinary)) . $valueBinary;
            }
        }

        return $binary;
    }

    #[\Override]
    public function getType(): Type {
        return Type::TUPLE;
    }

    /**
     * @return array<mixed> $value
     */
    #[\Override]
    public function getValue(): array {
        return $this->value;
    }

    #[\Override]
    final public static function requiresDefinition(): bool {
        return true;
    }
}
