<?php

declare(strict_types=1);

namespace Cassandra\TypeInfo;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\TypeInfoException;
use Cassandra\Type;
use Cassandra\ValueFactory;

final class MapCollectionInfo extends TypeInfo {
    public function __construct(
        public readonly TypeInfo $keyType,
        public readonly TypeInfo $valueType,
        public readonly bool $isFrozen,
    ) {
        parent::__construct(Type::MAP);
    }

    /**
     * @param array{
     *  type: \Cassandra\Type::MAP,
     *  keyType: \Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>),
     *  valueType: \Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>),
     *  isFrozen: bool,
     * } $typeDefinition
     * 
     * @throws \Cassandra\Exception\TypeInfoException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    public static function fromTypeDefinition(array $typeDefinition): self {
        if (!isset($typeDefinition['type'])) {
            throw new TypeInfoException(
                "MapCollection type definition is missing required 'type' property",
                ExceptionCode::TYPEINFO_MAP_MISSING_TYPE->value,
                [
                    'provided_keys' => array_keys($typeDefinition),
                    'required_keys' => ['type', 'keyType', 'valueType'],
                ]
            );
        }

        if ($typeDefinition['type'] !== Type::MAP) {
            throw new TypeInfoException(
                "Invalid type definition for MapCollection: 'type' must be Type::MAP",
                ExceptionCode::TYPEINFO_MAP_INVALID_TYPE->value,
            );
        }

        if (!isset($typeDefinition['keyType'])) {
            throw new TypeInfoException(
                "MapCollection type definition is missing required 'keyType' property",
                ExceptionCode::TYPEINFO_MAP_MISSING_KEYTYPE->value,
                [
                    'provided_keys' => array_keys($typeDefinition),
                    'required_keys' => ['type', 'keyType', 'valueType'],
                ]
            );
        }
        $keyType = ValueFactory::getTypeInfoFromTypeDefinition($typeDefinition['keyType']);

        if (!isset($typeDefinition['valueType'])) {
            throw new TypeInfoException(
                "MapCollection type definition is missing required 'valueType' property",
                ExceptionCode::TYPEINFO_MAP_MISSING_VALUETYPE->value,
                [
                    'provided_keys' => array_keys($typeDefinition),
                    'required_keys' => ['type', 'keyType', 'valueType'],
                ]
            );
        }
        $valueType = ValueFactory::getTypeInfoFromTypeDefinition($typeDefinition['valueType']);

        if (isset($typeDefinition['isFrozen']) && $typeDefinition['isFrozen'] === true) {
            $isFrozen = true;
        } else {
            $isFrozen = false;
        }

        return new self($keyType, $valueType, $isFrozen);
    }

}
