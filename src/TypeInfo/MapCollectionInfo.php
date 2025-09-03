<?php

declare(strict_types=1);

namespace Cassandra\TypeInfo;

use Cassandra\ExceptionCode;
use Cassandra\Type;
use Cassandra\TypeFactory;

final class MapCollectionInfo extends TypeInfo {
    public function __construct(
        public readonly TypeInfo $keyType,
        public readonly TypeInfo $valueType,
        public readonly bool $isFrozen,
    ) {
        parent::__construct(Type::MAP_COLLECTION);
    }

    /**
     * @param array{
     *  type: \Cassandra\Type::MAP_COLLECTION,
     *  keyType: \Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>),
     *  valueType: \Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>),
     *  isFrozen: bool,
     * } $typeDefinition
     * 
     * @throws \Cassandra\TypeInfo\Exception
     * @throws \Cassandra\Type\Exception
     */
    public static function fromTypeDefinition(array $typeDefinition): self {
        if (!isset($typeDefinition['type'])) {
            throw new Exception(
                "MapCollection type definition is missing required 'type' property",
                ExceptionCode::TYPEINFO_MAP_COLLECTION_MISSING_TYPE->value,
                [
                    'provided_keys' => array_keys($typeDefinition),
                    'required_keys' => ['type', 'keyType', 'valueType'],
                ]
            );
        }

        if ($typeDefinition['type'] !== Type::MAP_COLLECTION) {
            throw new Exception(
                "Invalid type definition for MapCollection: 'type' must be Type::MAP_COLLECTION",
                ExceptionCode::TYPEINFO_MAP_COLLECTION_INVALID_TYPE->value,
            );
        }

        if (!isset($typeDefinition['keyType'])) {
            throw new Exception(
                "MapCollection type definition is missing required 'keyType' property",
                ExceptionCode::TYPEINFO_MAP_COLLECTION_MISSING_KEYTYPE->value,
                [
                    'provided_keys' => array_keys($typeDefinition),
                    'required_keys' => ['type', 'keyType', 'valueType'],
                ]
            );
        }
        $keyType = TypeFactory::getTypeInfoFromTypeDefinition($typeDefinition['keyType']);

        if (!isset($typeDefinition['valueType'])) {
            throw new Exception(
                "MapCollection type definition is missing required 'valueType' property",
                ExceptionCode::TYPEINFO_MAP_COLLECTION_MISSING_VALUETYPE->value,
                [
                    'provided_keys' => array_keys($typeDefinition),
                    'required_keys' => ['type', 'keyType', 'valueType'],
                ]
            );
        }
        $valueType = TypeFactory::getTypeInfoFromTypeDefinition($typeDefinition['valueType']);

        if (isset($typeDefinition['isFrozen']) && $typeDefinition['isFrozen'] === true) {
            $isFrozen = true;
        } else {
            $isFrozen = false;
        }

        return new self($keyType, $valueType, $isFrozen);
    }

}
