<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\ExceptionCode;
use Cassandra\ValueFactory;
use Cassandra\Response\StreamReader;
use Cassandra\Type;
use Cassandra\TypeInfo\TupleInfo;
use Cassandra\TypeInfo\TypeInfo;

final class Tuple extends ValueReadableWithoutLength {
    protected TupleInfo $typeInfo;
    /**
     * @var array<mixed> $value
     */
    protected readonly array $value;

    /**
     * @param array<mixed> $value
     */
    final public function __construct(array $value, TupleInfo $typeInfo) {
        $this->value = $value;
        $this->typeInfo = $typeInfo;
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Value\Exception
     */
    #[\Override]
    public static function fromBinary(
        string $binary,
        ?TypeInfo $typeInfo = null,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): static {

        return self::fromStream(new StreamReader($binary), typeInfo: $typeInfo, valueEncodeConfig: $valueEncodeConfig);
    }

    /**
     * @param mixed $value
     * 
     * @throws \Cassandra\Value\Exception
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        if (!is_array($value)) {
            throw new Exception('Invalid tuple value; expected array', ExceptionCode::VALUE_TUPLE_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        if ($typeInfo === null) {
            throw new Exception('typeInfo is required', ExceptionCode::VALUE_TUPLE_TYPEINFO_REQUIRED->value);
        }

        if (!$typeInfo instanceof TupleInfo) {
            throw new Exception('Invalid type info, TupleInfo expected', ExceptionCode::VALUE_TUPLE_INVALID_TYPEINFO->value, [
                'given_type' => get_class($typeInfo),
            ]);
        }

        return new static($value, typeInfo: $typeInfo);
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Value\Exception
     */
    #[\Override]
    final public static function fromStream(
        StreamReader $stream,
        ?int $length = null,
        ?TypeInfo $typeInfo = null,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): static {
        if ($typeInfo === null) {
            throw new Exception('typeInfo is required', ExceptionCode::VALUE_TUPLE_TYPEINFO_REQUIRED->value);
        }

        if (!$typeInfo instanceof TupleInfo) {
            throw new Exception('Invalid type info, TupleInfo expected', ExceptionCode::VALUE_TUPLE_INVALID_TYPEINFO->value, [
                'given_type' => get_class($typeInfo),
            ]);
        }

        $valueEncodeConfig ??= ValueEncodeConfig::default();

        $tuple = [];
        foreach ($typeInfo->valueTypes as $key => $type) {
            /** @psalm-suppress MixedAssignment */
            $tuple[$key] = $stream->readValue($type, $valueEncodeConfig);
        }

        return new static($tuple, typeInfo: $typeInfo);
    }

    /**
     * @param array<mixed> $value
     * @param list<\Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>)> $valueDefinition
     *
     * @throws \Cassandra\Value\Exception
     * @throws \Cassandra\TypeInfo\Exception
     */
    final public static function fromValue(
        array $value,
        array $valueDefinition,
    ): static {

        return new static($value, TupleInfo::fromTypeDefinition([
            'type' => Type::TUPLE,
            'valueTypes' => $valueDefinition,
        ]));

    }

    /**
     * @throws \Cassandra\Value\Exception
     */
    #[\Override]
    public function getBinary(): string {
        $binary = '';
        $value = $this->value;

        foreach ($this->typeInfo->valueTypes as $key => $type) {
            if ($value[$key] === null) {
                $binary .= "\xff\xff\xff\xff";
            } else {
                $valueBinary = $value[$key] instanceof ValueBase
                    ? $value[$key]->getBinary()
                    : ValueFactory::getBinaryByTypeInfo($type, $value[$key]);

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
