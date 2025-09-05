<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\ExceptionCode;
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
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Value\Exception
     * @throws \Cassandra\Exception\VIntCodecException
     */
    #[\Override]
    public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static {

        return self::fromStream(new StreamReader($binary), typeInfo: $typeInfo);
    }

    /**
     * @param mixed $value
     * 
     * @throws \Cassandra\Value\Exception
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        if (!is_array($value)) {
            throw new Exception('Invalid tuple value; expected array', ExceptionCode::VALUE_VECTOR_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        if ($typeInfo === null) {
            throw new Exception('typeInfo is required', ExceptionCode::VALUE_VECTOR_TYPEINFO_REQUIRED->value);
        }

        if (!$typeInfo instanceof VectorInfo) {
            throw new Exception('Invalid type info, VectorInfo expected', ExceptionCode::VALUE_VECTOR_INVALID_TYPEINFO->value, [
                'given_type' => get_class($typeInfo),
            ]);
        }

        return new static($value, typeInfo: $typeInfo);
    }

    /**
     * @throws \Cassandra\Value\Exception
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Exception\VIntCodecException
     */
    #[\Override]
    final public static function fromStream(StreamReader $stream, ?int $length = null, ?TypeInfo $typeInfo = null): static {
        if ($typeInfo === null) {
            throw new Exception('typeInfo is required', ExceptionCode::VALUE_VECTOR_TYPEINFO_REQUIRED->value);
        }

        if (!$typeInfo instanceof VectorInfo) {
            throw new Exception('Invalid type info, VectorInfo expected', ExceptionCode::VALUE_VECTOR_INVALID_TYPEINFO->value, [
                'given_type' => get_class($typeInfo),
            ]);
        }

        $vector = [];

        $valueType = $typeInfo->valueType;

        $serializedLength = ValueFactory::getSerializedLengthOfType($valueType->type);

        if ($serializedLength > 0) {
            for ($i = 0; $i < $typeInfo->dimensions; ++$i) {

                $typeObject = ValueFactory::getValueObjectFromStream($valueType, $serializedLength, $stream);

                /** @psalm-suppress MixedAssignment */
                $vector[] = $typeObject->getValue();

            }
        } else {
            for ($i = 0; $i < $typeInfo->dimensions; ++$i) {

                $serializedLength = $stream->readUnsignedVint32();
                $typeObject = ValueFactory::getValueObjectFromStream($valueType, $serializedLength, $stream);

                /** @psalm-suppress MixedAssignment */
                $vector[] = $typeObject->getValue();
            }
        }

        return new static($vector, typeInfo: $typeInfo);
    }

    /**
     * @param array<mixed> $value
     * @param \Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>) $valueDefinition
     *
     * @throws \Cassandra\Value\Exception
     * @throws \Cassandra\TypeInfo\Exception
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
     * @throws \Cassandra\Value\Exception
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
