<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\ExceptionCode;
use Cassandra\TypeFactory;
use Cassandra\Response\StreamReader;
use Cassandra\Type;
use Cassandra\TypeInfo\VectorInfo;
use Cassandra\TypeInfo\TypeInfo;

final class Vector extends TypeReadableWithoutLength {
    protected VectorInfo $typeInfo;
    /**
     * @var array<mixed> $value
     */
    protected readonly array $value;

    /**
     * @param array<mixed> $value
     * @param \Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>)|null $valueDefinition
     *
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\TypeInfo\Exception
     */
    final public function __construct(array $value, Type|array|null $valueDefinition = null, int|null $dimensions = null, ?VectorInfo $typeInfo = null) {
        $this->value = $value;

        if ($valueDefinition !== null && $dimensions !== null) {
            $this->typeInfo = VectorInfo::fromTypeDefinition([
                'type' => Type::VECTOR,
                'valueType' => $valueDefinition,
                'dimensions' => $dimensions,
            ]);
        } elseif ($typeInfo !== null) {
            $this->typeInfo = $typeInfo;
        } else {
            throw new Exception('Either valueDefinition and dimensions or typeInfo must be provided', ExceptionCode::TYPE_VECTOR_VALUEDEF_DIM_OR_TYPEINFO_REQUIRED->value);
        }
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\TypeInfo\Exception
     * @throws \Cassandra\Exception
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
            throw new Exception('Invalid tuple value; expected array', ExceptionCode::TYPE_VECTOR_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        if ($typeInfo === null) {
            throw new Exception('typeInfo is required', ExceptionCode::TYPE_VECTOR_TYPEINFO_REQUIRED->value);
        }

        if (!$typeInfo instanceof VectorInfo) {
            throw new Exception('Invalid type info, VectorInfo expected', ExceptionCode::TYPE_VECTOR_INVALID_TYPEINFO->value, [
                'given_type' => get_class($typeInfo),
            ]);
        }

        return new static($value, typeInfo: $typeInfo);
    }

    /**
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\TypeInfo\Exception
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Exception
     */
    #[\Override]
    final public static function fromStream(StreamReader $stream, ?int $length = null, ?TypeInfo $typeInfo = null): static {
        if ($typeInfo === null) {
            throw new Exception('typeInfo is required', ExceptionCode::TYPE_VECTOR_TYPEINFO_REQUIRED->value);
        }

        if (!$typeInfo instanceof VectorInfo) {
            throw new Exception('Invalid type info, VectorInfo expected', ExceptionCode::TYPE_VECTOR_INVALID_TYPEINFO->value, [
                'given_type' => get_class($typeInfo),
            ]);
        }

        $vector = [];

        $valueType = $typeInfo->valueType;

        $serializedLength = TypeFactory::getSerializedLengthOfType($valueType->type);

        if ($serializedLength > 0) {
            for ($i = 0; $i < $typeInfo->dimensions; ++$i) {

                $typeObject = TypeFactory::getTypeObjectFromStream($valueType, $serializedLength, $stream);

                /** @psalm-suppress MixedAssignment */
                $vector[] = $typeObject->getValue();

            }
        } else {
            for ($i = 0; $i < $typeInfo->dimensions; ++$i) {

                $serializedLength = $stream->readUnsignedVint32();
                $typeObject = TypeFactory::getTypeObjectFromStream($valueType, $serializedLength, $stream);

                /** @psalm-suppress MixedAssignment */
                $vector[] = $typeObject->getValue();
            }
        }

        return new static($vector, typeInfo: $typeInfo);
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public function getBinary(): string {
        $binary = '';
        $value = $this->value;

        $isSerializedAsFixedLength = TypeFactory::isSerializedAsFixedLength($this->typeInfo->valueType->type);

        for ($i = 0; $i < $this->typeInfo->dimensions; ++$i) {
            if ($value[$i] === null) {
                // todo: test if this is possible
                $binary .= "\xff\xff\xff\xff";
            } else {
                $valueBinary = $value[$i] instanceof TypeBase
                    ? $value[$i]->getBinary()
                    : TypeFactory::getBinaryByTypeInfo($this->typeInfo->valueType, $value[$i]);

                if ($isSerializedAsFixedLength) {
                    $binary .= $valueBinary;
                } else {
                    $length = strlen($valueBinary);
                    $lengthBinary = (new Varint($length))->getBinary();
                    $binary .= $lengthBinary . $valueBinary;
                }
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
