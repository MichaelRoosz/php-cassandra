<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\TypeFactory;
use Cassandra\Response\StreamReader;
use Cassandra\Type;
use Cassandra\TypeInfo\CollectionSetInfo;
use Cassandra\TypeInfo\TypeInfo;

final class CollectionSet extends TypeBase {
    protected CollectionSetInfo $typeInfo;

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
    final public function __construct(array $value, Type|array|null $valueDefinition = null, ?CollectionSetInfo $typeInfo = null) {
        $this->value = $value;

        if ($valueDefinition !== null) {
            $this->typeInfo = CollectionSetInfo::fromTypeDefinition([
                'type' => Type::COLLECTION_SET,
                'valueType' => $valueDefinition,
            ]);
        } elseif ($typeInfo !== null) {
            $this->typeInfo = $typeInfo;
        } else {
            throw new Exception('Either valueDefinition or typeInfo must be provided', Exception::CODE_COLLECTION_SET_VALUEDEF_OR_TYPEINFO_REQUIRED);
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
            throw new Exception('typeInfo is required', Exception::CODE_COLLECTION_SET_TYPEINFO_REQUIRED);
        }

        if (!$typeInfo instanceof CollectionSetInfo) {
            throw new Exception('Invalid type info, CollectionSetInfo expected', Exception::CODE_COLLECTION_SET_INVALID_TYPEINFO, [
                'given_type' => get_class($typeInfo),
            ]);
        }

        return new static((new StreamReader($binary))->readSet($typeInfo), typeInfo: $typeInfo);
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
            throw new Exception('Invalid set value; expected array', Exception::CODE_COLLECTION_SET_INVALID_VALUE_TYPE, [
                'value_type' => gettype($value),
            ]);
        }

        if ($typeInfo === null) {
            throw new Exception('typeInfo is required', Exception::CODE_COLLECTION_SET_TYPEINFO_REQUIRED);
        }

        if (!$typeInfo instanceof CollectionSetInfo) {
            throw new Exception('Invalid type info, CollectionSetInfo expected', Exception::CODE_COLLECTION_SET_INVALID_TYPEINFO, [
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
