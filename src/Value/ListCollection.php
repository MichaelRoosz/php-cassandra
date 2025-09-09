<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\ValueFactory;
use Cassandra\Response\StreamReader;
use Cassandra\Type;
use Cassandra\TypeInfo\ListCollectionInfo;
use Cassandra\TypeInfo\TypeInfo;

final class ListCollection extends ValueReadableWithoutLength {
    protected ListCollectionInfo $typeInfo;

    /**
     * @var array<mixed> $value
     */
    protected readonly array $value;

    /**
     * @param array<mixed> $value
     */
    final public function __construct(
        array $value,
        ListCollectionInfo $typeInfo,
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
            throw new ValueException('Invalid list value; expected array', ExceptionCode::VALUE_LIST_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        if ($typeInfo === null) {
            throw new ValueException('typeInfo is required', ExceptionCode::VALUE_LIST_TYPEINFO_REQUIRED->value);
        }

        if (!$typeInfo instanceof ListCollectionInfo) {
            throw new ValueException('Invalid type info, ListCollectionInfo expected', ExceptionCode::VALUE_LIST_INVALID_TYPEINFO->value, [
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
            throw new ValueException('typeInfo is required', ExceptionCode::VALUE_LIST_TYPEINFO_REQUIRED->value);
        }

        if (!$typeInfo instanceof ListCollectionInfo) {
            throw new ValueException('Invalid type info, ListCollectionInfo expected', ExceptionCode::VALUE_LIST_INVALID_TYPEINFO->value, [
                'given_type' => get_class($typeInfo),
            ]);
        }

        $valueEncodeConfig ??= ValueEncodeConfig::default();

        $list = [];
        $count = $stream->readInt();
        for ($i = 0; $i < $count; ++$i) {
            /** @psalm-suppress MixedAssignment */
            $list[] = $stream->readValue($typeInfo->valueType, $valueEncodeConfig);
        }

        return new static($list, typeInfo: $typeInfo);
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

        return new static($value, ListCollectionInfo::fromTypeDefinition([
            'type' => Type::LIST,
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
        $binary = pack('N', count($this->value));

        /** @var mixed $val */
        foreach ($this->value as $val) {
            $itemPacked = ValueFactory::getBinaryByTypeInfo($this->typeInfo->valueType, $val);
            $binary .= pack('N', strlen($itemPacked)) . $itemPacked;
        }

        return $binary;
    }

    #[\Override]
    public function getType(): Type {
        return Type::LIST;
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
}
