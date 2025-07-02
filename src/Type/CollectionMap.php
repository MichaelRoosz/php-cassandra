<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\TypeFactory;
use Cassandra\Response\StreamReader;
use Cassandra\Type;
use Cassandra\TypeInfo\CollectionMapInfo;
use Cassandra\TypeInfo\TypeInfo;

final class CollectionMap extends TypeBase {
    protected CollectionMapInfo $typeInfo;

    /**
     * @var array<mixed> $value
     */
    protected array $value;

    /**
     * @param array<mixed> $value
     * @param \Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>)|null $keyDefinition
     * @param \Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>)|null $valueDefinition
     *
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\TypeInfo\Exception
     */
    final public function __construct(
        array $value,
        Type|array|null $keyDefinition = null,
        Type|array|null $valueDefinition = null,
        ?CollectionMapInfo $typeInfo = null
    ) {

        if ($typeInfo !== null) {
            $this->typeInfo = $typeInfo;
        } elseif ($keyDefinition !== null && $valueDefinition !== null) {
            $this->typeInfo = CollectionMapInfo::fromTypeDefinition([
                'type' => Type::COLLECTION_MAP,
                'keyType' => $keyDefinition,
                'valueType' => $valueDefinition,
            ]);
        } else {
            throw new Exception('Either keyDefinition and valueDefinition or typeInfo must be provided');
        }

        $this->value = $value;
    }

    /**
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\TypeInfo\Exception
     */
    #[\Override]
    public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static {
        if ($typeInfo === null) {
            throw new Exception('typeInfo is required');
        }

        if (!$typeInfo instanceof CollectionMapInfo) {
            throw new Exception('Invalid type info, CollectionMapInfo expected');
        }

        return new static((new StreamReader($binary))->readMap($typeInfo), typeInfo: $typeInfo);
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

        if (!$typeInfo instanceof CollectionMapInfo) {
            throw new Exception('Invalid type info, CollectionMapInfo expected');
        }

        return new static($value, typeInfo: $typeInfo);
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public function getBinary(): string {

        $binary = pack('N', count($this->value));

        /** @var TypeBase|mixed $val */
        foreach ($this->value as $key => $val) {
            $keyPacked = TypeFactory::getBinaryByTypeInfo($this->typeInfo->keyType, $key);

            $valuePacked = $val instanceof TypeBase
                ? $val->getBinary()
                : TypeFactory::getBinaryByTypeInfo($this->typeInfo->valueType, $val);

            $binary .= pack('N', strlen($keyPacked)) . $keyPacked;
            $binary .= pack('N', strlen($valuePacked)) . $valuePacked;
        }

        return $binary;
    }

    /**
     * @return array<mixed> $value
     */
    #[\Override]
    public function getValue(): array {
        return $this->value;
    }
}
