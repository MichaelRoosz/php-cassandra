<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\ValueFactory;
use Cassandra\Response\StreamReader;
use Cassandra\Type;
use Cassandra\TypeInfo\VectorInfo;
use Cassandra\TypeInfo\TypeInfo;

final class Vector extends ValueReadableWithoutLength {
    protected VectorInfo $typeInfo;
    /**
     * @var array<mixed> $value
     */
    protected readonly array $value;

    /**
     * @param array<mixed> $value
     */
    final public function __construct(array $value, VectorInfo $typeInfo) {
        $this->value = $value;
        $this->typeInfo = $typeInfo;
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\VIntCodecException
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
            throw new ValueException('Invalid tuple value; expected array', ExceptionCode::VALUE_VECTOR_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        if ($typeInfo === null) {
            throw new ValueException('typeInfo is required', ExceptionCode::VALUE_VECTOR_TYPEINFO_REQUIRED->value);
        }

        if (!$typeInfo instanceof VectorInfo) {
            throw new ValueException('Invalid type info, VectorInfo expected', ExceptionCode::VALUE_VECTOR_INVALID_TYPEINFO->value, [
                'given_type' => get_class($typeInfo),
            ]);
        }

        return new static($value, typeInfo: $typeInfo);
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\VIntCodecException
     */
    #[\Override]
    final public static function fromStream(
        StreamReader $stream,
        ?int $length = null,
        ?TypeInfo $typeInfo = null,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): static {
        if ($typeInfo === null) {
            throw new ValueException('typeInfo is required', ExceptionCode::VALUE_VECTOR_TYPEINFO_REQUIRED->value);
        }

        if (!$typeInfo instanceof VectorInfo) {
            throw new ValueException('Invalid type info, VectorInfo expected', ExceptionCode::VALUE_VECTOR_INVALID_TYPEINFO->value, [
                'given_type' => get_class($typeInfo),
            ]);
        }

        $valueEncodeConfig ??= ValueEncodeConfig::default();

        $vector = [];

        $valueType = $typeInfo->valueType;

        $serializedLength = ValueFactory::getSerializedLengthOfType($valueType->type);

        if ($serializedLength > 0) {
            for ($i = 0; $i < $typeInfo->dimensions; ++$i) {

                $valueObject = ValueFactory::getValueObjectFromStream($valueType, $serializedLength, $stream);

                if ($valueObject instanceof ValueWithMultipleEncodings) {
                    /** @psalm-suppress MixedAssignment */
                    $vector[] = $valueObject->asConfigured($valueEncodeConfig);
                } else {
                    /** @psalm-suppress MixedAssignment */
                    $vector[] = $valueObject->getValue();
                }
            }
        } else {
            for ($i = 0; $i < $typeInfo->dimensions; ++$i) {

                $serializedLength = $stream->readUnsignedVint32();
                $valueObject = ValueFactory::getValueObjectFromStream($valueType, $serializedLength, $stream);

                if ($valueObject instanceof ValueWithMultipleEncodings) {
                    /** @psalm-suppress MixedAssignment */
                    $vector[] = $valueObject->asConfigured($valueEncodeConfig);
                } else {
                    /** @psalm-suppress MixedAssignment */
                    $vector[] = $valueObject->getValue();
                }
            }
        }

        return new static($vector, typeInfo: $typeInfo);
    }

    /**
     * @param array<mixed> $value
     * @param \Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>) $valueDefinition
     *
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\TypeInfoException
     */
    final public static function fromValue(
        array $value,
        Type|array $valueDefinition,
        int $dimensions,
    ): static {

        return new static($value, VectorInfo::fromTypeDefinition([
            'type' => Type::VECTOR,
            'valueType' => $valueDefinition,
            'dimensions' => $dimensions,
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

        $isSerializedAsFixedLength = ValueFactory::isSerializedAsFixedLength($this->typeInfo->valueType->type);

        for ($i = 0; $i < $this->typeInfo->dimensions; ++$i) {
            $valueBinary = $value[$i] instanceof ValueBase
                ? $value[$i]->getBinary()
                : ValueFactory::getBinaryByTypeInfo($this->typeInfo->valueType, $value[$i]);

            if ($isSerializedAsFixedLength) {
                $binary .= $valueBinary;
            } else {
                $length = strlen($valueBinary);
                $lengthBinary = (new Varint($length))->getBinary();
                $binary .= $lengthBinary . $valueBinary;
            }
        }

        return $binary;
    }

    #[\Override]
    public function getType(): Type {
        return Type::VECTOR;
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
