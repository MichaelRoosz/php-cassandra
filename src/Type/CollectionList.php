<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\ExceptionCode;
use Cassandra\TypeFactory;
use Cassandra\Response\StreamReader;
use Cassandra\Type;
use Cassandra\TypeInfo\CollectionListInfo;
use Cassandra\TypeInfo\TypeInfo;

final class CollectionList extends TypeReadableWithoutLength {
    protected CollectionListInfo $typeInfo;

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
    final public function __construct(
        array $value,
        Type|array|null $valueDefinition = null,
        bool $isFrozen = false,
        ?CollectionListInfo $typeInfo = null,
    ) {
        $this->value = $value;

        if ($typeInfo !== null) {
            $this->typeInfo = $typeInfo;

        } elseif ($valueDefinition !== null) {
            $this->typeInfo = CollectionListInfo::fromTypeDefinition([
                'type' => Type::COLLECTION_LIST,
                'valueType' => $valueDefinition,
                'isFrozen' => $isFrozen,
            ]);

        } else {
            throw new Exception('Either valueDefinition or typeInfo must be provided', ExceptionCode::TYPE_COLLECTION_LIST_VALUEDEF_OR_TYPEINFO_REQUIRED->value);
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
            throw new Exception('Invalid list value; expected array', ExceptionCode::TYPE_COLLECTION_LIST_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        if ($typeInfo === null) {
            throw new Exception('typeInfo is required', ExceptionCode::TYPE_COLLECTION_LIST_TYPEINFO_REQUIRED->value);
        }

        if (!$typeInfo instanceof CollectionListInfo) {
            throw new Exception('Invalid type info, CollectionListInfo expected', ExceptionCode::TYPE_COLLECTION_LIST_INVALID_TYPEINFO->value, [
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
            throw new Exception('typeInfo is required', ExceptionCode::TYPE_COLLECTION_LIST_TYPEINFO_REQUIRED->value);
        }

        if (!$typeInfo instanceof CollectionListInfo) {
            throw new Exception('Invalid type info, CollectionListInfo expected', ExceptionCode::TYPE_COLLECTION_LIST_INVALID_TYPEINFO->value, [
                'given_type' => get_class($typeInfo),
            ]);
        }

        $list = [];
        $count = $stream->readInt();
        for ($i = 0; $i < $count; ++$i) {
            /** @psalm-suppress MixedAssignment */
            $list[] = $stream->readValue($typeInfo->valueType);
        }

        return new static($list, typeInfo: $typeInfo);
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public function getBinary(): string {
        $binary = pack('N', count($this->value));

        /** @var mixed $val */
        foreach ($this->value as $val) {
            $itemPacked = TypeFactory::getBinaryByTypeInfo($this->typeInfo->valueType, $val);
            $binary .= pack('N', strlen($itemPacked)) . $itemPacked;
        }

        return $binary;
    }

    #[\Override]
    public function getType(): Type {
        return Type::COLLECTION_LIST;
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
