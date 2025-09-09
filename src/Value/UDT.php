<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\ValueFactory;
use Cassandra\Response\StreamReader;
use Cassandra\Type;
use Cassandra\TypeInfo\UDTInfo;
use Cassandra\TypeInfo\TypeInfo;

final class UDT extends ValueReadableWithoutLength {
    protected UDTInfo $typeInfo;
    /**
     * @var array<mixed> $value
     */
    protected readonly array $value;

    /**
     * @param array<mixed> $value
     */
    final public function __construct(
        array $value,
        UDTInfo $typeInfo ,
    ) {
        $this->value = $value;
        $this->typeInfo = $typeInfo;
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
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
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        if (!is_array($value)) {
            throw new ValueException('Invalid UDT value; expected associative array', ExceptionCode::VALUE_UDT_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        if ($typeInfo === null) {
            throw new ValueException('typeInfo is required', ExceptionCode::VALUE_UDT_TYPEINFO_REQUIRED->value);
        }

        if (!$typeInfo instanceof UDTInfo) {
            throw new ValueException('Invalid type info, UDTInfo expected', ExceptionCode::VALUE_UDT_INVALID_TYPEINFO->value, [
                'given_type' => get_class($typeInfo),
            ]);
        }

        return new static($value, typeInfo: $typeInfo);
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    #[\Override]
    final public static function fromStream(
        StreamReader $stream,
        ?int $length = null,
        ?TypeInfo $typeInfo = null,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): static {

        if ($typeInfo === null) {
            throw new ValueException('typeInfo is required', ExceptionCode::VALUE_UDT_TYPEINFO_REQUIRED->value);
        }

        if (!$typeInfo instanceof UDTInfo) {
            throw new ValueException('Invalid type info, UDTInfo expected', ExceptionCode::VALUE_UDT_INVALID_TYPEINFO->value, [
                'given_type' => get_class($typeInfo),
            ]);
        }

        $valueEncodeConfig ??= ValueEncodeConfig::default();

        $udt = [];
        foreach ($typeInfo->valueTypes as $key => $type) {
            /** @psalm-suppress MixedAssignment */
            $udt[$key] = $stream->readValue($type, $valueEncodeConfig);
        }

        return new static($udt, typeInfo: $typeInfo);
    }

    /**
     * @param array<mixed> $value
     * @param array<string,\Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>)> $valueDefinition 
     *
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\TypeInfoException
     */
    final public static function fromValue(
        array $value,
        array $valueDefinition,
        bool $isFrozen = false,
    ): static {

        return new static($value, UDTInfo::fromTypeDefinition([
            'type' => Type::UDT,
            'valueTypes' => $valueDefinition,
            'isFrozen' => $isFrozen,
        ]));

    }

    /**
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
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
        return Type::UDT;
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
