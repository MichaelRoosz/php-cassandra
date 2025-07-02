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
     * @param Type|(array{ type: Type }&array<mixed>)|null $valueDefinition
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
            throw new Exception('Either valueDefinition or typeInfo must be provided');
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
            throw new Exception('typeInfo is required');
        }

        if (!$typeInfo instanceof CollectionListInfo) {
            throw new Exception('Invalid type info, CollectionListInfo expected');
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
            throw new Exception('Invalid value');
        }

        if ($typeInfo === null) {
            throw new Exception('typeInfo is required');
        }

        if (!$typeInfo instanceof CollectionListInfo) {
            throw new Exception('Invalid type info, CollectionListInfo expected');
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
