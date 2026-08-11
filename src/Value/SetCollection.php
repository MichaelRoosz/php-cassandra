<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\ValueFactory;
use Cassandra\Response\StreamReader;
use Cassandra\Type;
use Cassandra\TypeInfo\SetCollectionInfo;
use Cassandra\TypeInfo\TypeInfo;

final class SetCollection extends ValueReadableWithoutLength {
    private SetCollectionInfo $typeInfo;

    /**
     * @var array<mixed> $value
     */
    private readonly array $value;

    /**
     * @param array<mixed> $value
     */
    final public function __construct(
        array $value,
        SetCollectionInfo $typeInfo,
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
            throw new ValueException('Invalid set value; expected array', ExceptionCode::VALUE_SET_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        if ($typeInfo === null) {
            throw new ValueException('typeInfo is required', ExceptionCode::VALUE_SET_TYPEINFO_REQUIRED->value);
        }

        if (!$typeInfo instanceof SetCollectionInfo) {
            throw new ValueException('Invalid type info, SetCollectionInfo expected', ExceptionCode::VALUE_SET_INVALID_TYPEINFO->value, [
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
            throw new ValueException('typeInfo is required', ExceptionCode::VALUE_SET_TYPEINFO_REQUIRED->value);
        }

        if (!$typeInfo instanceof SetCollectionInfo) {
            throw new ValueException('Invalid type info, SetCollectionInfo expected', ExceptionCode::VALUE_SET_INVALID_TYPEINFO->value, [
                'given_type' => get_class($typeInfo),
            ]);
        }

        $valueEncodeConfig ??= ValueEncodeConfig::default();

        $set = [];
        $count = $stream->readInt();
        $maximumCount = intdiv(max(0, $stream->remainingLength()), 4);
        if ($count < 0 || $count > $maximumCount) {
            throw new ValueException(
                'Set element count does not fit in the available value data',
                ExceptionCode::VALUE_SET_INVALID_VALUE_TYPE->value,
                ['count' => $count, 'maximum_count' => $maximumCount]
            );
        }
        for ($i = 0; $i < $count; ++$i) {
            /** @psalm-suppress MixedAssignment */
            $set[] = $stream->readValue($typeInfo->valueType, $valueEncodeConfig);
        }

        return new static($set, typeInfo: $typeInfo);
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
        bool $isFrozen = false,
    ): static {

        return new static($value, SetCollectionInfo::fromTypeDefinition([
            'type' => Type::SET,
            'valueType' => $valueDefinition,
            'isFrozen' => $isFrozen,
        ]));
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    #[\Override]
    public function getBinary(): string {
        /** @var list<array{index: int|string, binary: string}> $elements */
        $elements = [];

        /** @var mixed $val */
        foreach ($this->value as $index => $val) {
            if ($val === null) {
                throw new ValueException(
                    'A set element cannot be null; CQL has no null inside a collection, so leave the element out instead',
                    ExceptionCode::VALUE_SET_NULL_ELEMENT->value,
                    [
                        'index' => $index,
                        'value_type' => $this->typeInfo->valueType->type->name,
                    ]
                );
            }

            // Re-encode a value object against the set's declared type. Besides
            // keeping raw and object inputs identical, this guarantees the
            // comparator below never interprets one type's bytes as another.
            $itemPacked = $val instanceof ValueBase
                ? ValueFactory::getBinaryByTypeInfo($this->typeInfo->valueType, $val->getValue())
                : ValueFactory::getBinaryByTypeInfo($this->typeInfo->valueType, $val);
            $elements[] = ['index' => $index, 'binary' => $itemPacked];
        }

        usort($elements, $this->compareElements(...));

        $binary = pack('N', count($elements));
        $previous = null;
        foreach ($elements as $element) {
            if (
                $previous !== null
                && ValueComparator::compare($this->typeInfo->valueType, $previous['binary'], $element['binary']) === 0
            ) {
                throw new ValueException(
                    'A set cannot contain the same CQL value more than once',
                    ExceptionCode::VALUE_SET_DUPLICATE_ELEMENT->value,
                    [
                        'first_index' => $previous['index'],
                        'duplicate_index' => $element['index'],
                        'value_type' => $this->typeInfo->valueType->type->name,
                    ]
                );
            }

            $binary .= pack('N', strlen($element['binary'])) . $element['binary'];
            $previous = $element;
        }

        return $binary;
    }

    #[\Override]
    public function getType(): Type {
        return Type::SET;
    }

    /**
     * @return array<mixed>
     */
    #[\Override]
    public function getValue(): array {
        return $this->value;
    }

    #[\Override]
    final public static function requiresDefinition(): bool {
        return true;
    }

    /**
     * @param array{index: int|string, binary: string} $left
     * @param array{index: int|string, binary: string} $right
     *
     * @throws \Cassandra\Exception\ValueException
     */
    private function compareElements(array $left, array $right): int {
        return ValueComparator::compare(
            $this->typeInfo->valueType,
            $left['binary'],
            $right['binary'],
        );
    }
}
