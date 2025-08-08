<?php

declare(strict_types=1);

namespace Cassandra\TypeInfo;

use Cassandra\Type;
use Cassandra\TypeFactory;

final class CollectionMapInfo extends TypeInfo {
    public function __construct(
        public readonly TypeInfo $keyType,
        public readonly TypeInfo $valueType,
    ) {
        parent::__construct(Type::COLLECTION_MAP);
    }

    /**
     * @param array{
     *  type: \Cassandra\Type::COLLECTION_MAP,
     *  keyType: \Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>),
     *  valueType: \Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>),
     * } $typeDefinition
     * 
     * @throws \Cassandra\TypeInfo\Exception
     * @throws \Cassandra\Type\Exception
     */
    public static function fromTypeDefinition(array $typeDefinition): self {
        if (!isset($typeDefinition['type'])) {
            throw new Exception(
                "CollectionMap type definition is missing required 'type' property",
                Exception::COLLECTION_MAP_MISSING_TYPE,
                [
                    'provided_keys' => array_keys($typeDefinition),
                    'required_keys' => ['type', 'keyType', 'valueType'],
                ]
            );
        }

        if ($typeDefinition['type'] !== Type::COLLECTION_MAP) {
            throw new Exception(
                "Invalid type definition for CollectionMap: 'type' must be Type::COLLECTION_MAP",
                Exception::COLLECTION_MAP_INVALID_TYPE,
            );
        }

        if (!isset($typeDefinition['keyType'])) {
            throw new Exception(
                "CollectionMap type definition is missing required 'keyType' property",
                Exception::COLLECTION_MAP_MISSING_KEYTYPE,
                [
                    'provided_keys' => array_keys($typeDefinition),
                    'required_keys' => ['type', 'keyType', 'valueType'],
                ]
            );
        }
        $keyType = TypeFactory::getTypeInfoFromTypeDefinition($typeDefinition['keyType']);

        if (!isset($typeDefinition['valueType'])) {
            throw new Exception(
                "CollectionMap type definition is missing required 'valueType' property",
                Exception::COLLECTION_MAP_MISSING_VALUETYPE,
                [
                    'provided_keys' => array_keys($typeDefinition),
                    'required_keys' => ['type', 'keyType', 'valueType'],
                ]
            );
        }
        $valueType = TypeFactory::getTypeInfoFromTypeDefinition($typeDefinition['valueType']);

        return new self($keyType, $valueType);
    }

}
