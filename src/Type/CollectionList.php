<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\TypeFactory;
use Cassandra\Response\StreamReader;
use Cassandra\Type;
use Cassandra\TypeInfo\CollectionListInfo;
use Cassandra\TypeInfo\TypeInfo;

final class CollectionList extends TypeBase {
    protected CollectionListInfo $typeInfo;

    /**
     * @var array<mixed> $value
     */
    protected array $value;

    /**
     * @param array<mixed> $value
     * @param \Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>)|null $valueDefinition
     *
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\TypeInfo\Exception
     */
    final public function __construct(array $value, Type|array|null $valueDefinition = null, ?CollectionListInfo $typeInfo = null) {
        $this->value = $value;

        if ($typeInfo !== null) {
            $this->typeInfo = $typeInfo;

        } elseif ($valueDefinition !== null) {
            $this->typeInfo = CollectionListInfo::fromTypeDefinition([
                'type' => Type::COLLECTION_LIST,
                'valueType' => $valueDefinition,
            ]);

        } else {
            throw new Exception('Either valueDefinition or typeInfo must be provided', Exception::CODE_COLLECTION_LIST_VALUEDEF_OR_TYPEINFO_REQUIRED);
        }
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\TypeInfo\Exception
     */
    #[\Override]
    public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static {
        if ($typeInfo === null) {
            throw new Exception('typeInfo is required', Exception::CODE_COLLECTION_LIST_TYPEINFO_REQUIRED);
        }

        if (!$typeInfo instanceof CollectionListInfo) {
            throw new Exception('Invalid type info, CollectionListInfo expected', Exception::CODE_COLLECTION_LIST_INVALID_TYPEINFO, [
                'given_type' => get_class($typeInfo),
            ]);
        }

        return new static((new StreamReader($binary))->readList($typeInfo), typeInfo: $typeInfo);
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
            throw new Exception('Invalid list value; expected array', Exception::CODE_COLLECTION_LIST_INVALID_VALUE_TYPE, [
                'value_type' => gettype($value),
            ]);
        }

        if ($typeInfo === null) {
            throw new Exception('typeInfo is required', Exception::CODE_COLLECTION_LIST_TYPEINFO_REQUIRED);
        }

        if (!$typeInfo instanceof CollectionListInfo) {
            throw new Exception('Invalid type info, CollectionListInfo expected', Exception::CODE_COLLECTION_LIST_INVALID_TYPEINFO, [
                'given_type' => get_class($typeInfo),
            ]);
        }

        return new static($value, typeInfo: $typeInfo);
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

    /**
     * @return array<mixed>
     */
    #[\Override]
    public function getValue(): array {
        return $this->value;
    }
}
