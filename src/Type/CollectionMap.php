<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\ExceptionCode;
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
    protected readonly array $value;

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
        bool $isFrozen = false,
        ?CollectionMapInfo $typeInfo = null,
    ) {

        if ($typeInfo !== null) {
            $this->typeInfo = $typeInfo;
        } elseif ($keyDefinition !== null && $valueDefinition !== null) {
            $this->typeInfo = CollectionMapInfo::fromTypeDefinition([
                'type' => Type::COLLECTION_MAP,
                'keyType' => $keyDefinition,
                'valueType' => $valueDefinition,
                'isFrozen' => $isFrozen,
            ]);
        } else {
            throw new Exception('Either keyDefinition and valueDefinition or typeInfo must be provided', ExceptionCode::TYPE_COLLECTION_MAP_KEY_VALUEDEF_OR_TYPEINFO_REQUIRED->value);
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
            throw new Exception('typeInfo is required', ExceptionCode::TYPE_COLLECTION_MAP_TYPEINFO_REQUIRED->value);
        }

        if (!$typeInfo instanceof CollectionMapInfo) {
            throw new Exception('Invalid type info, CollectionMapInfo expected', ExceptionCode::TYPE_COLLECTION_MAP_INVALID_TYPEINFO->value, [
                'given_type' => get_class($typeInfo),
            ]);
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
            throw new Exception('Invalid map value; expected array', ExceptionCode::TYPE_COLLECTION_MAP_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        if ($typeInfo === null) {
            throw new Exception('typeInfo is required', ExceptionCode::TYPE_COLLECTION_MAP_TYPEINFO_REQUIRED->value);
        }

        if (!$typeInfo instanceof CollectionMapInfo) {
            throw new Exception('Invalid type info, CollectionMapInfo expected', ExceptionCode::TYPE_COLLECTION_MAP_INVALID_TYPEINFO->value, [
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

    #[\Override]
    public function getType(): Type {
        return Type::COLLECTION_MAP;
    }

    /**
     * @return array<mixed> $value
     */
    #[\Override]
    public function getValue(): array {
        return $this->value;
    }
}
