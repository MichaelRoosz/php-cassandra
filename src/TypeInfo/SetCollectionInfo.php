<?php

declare(strict_types=1);

namespace Cassandra\TypeInfo;

use Cassandra\ExceptionCode;
use Cassandra\Type;
use Cassandra\TypeFactory;

final class SetCollectionInfo extends TypeInfo {
    public function __construct(
        public readonly TypeInfo $valueType,
        public readonly bool $isFrozen,
    ) {
        parent::__construct(Type::SET_COLLECTION);
    }

    /**
     * @param array{
     *  type: \Cassandra\Type::SET_COLLECTION,
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
                "SetCollection type definition is missing required 'type' property",
                ExceptionCode::TYPEINFO_SET_COLLECTION_MISSING_TYPE->value,
                [
                    'provided_keys' => array_keys($typeDefinition),
                    'required_keys' => ['type', 'valueType'],
                ]
            );
        }

        if ($typeDefinition['type'] !== Type::SET_COLLECTION) {
            throw new Exception(
                "Invalid type definition for SetCollection: 'type' must be Type::SET_COLLECTION",
                ExceptionCode::TYPEINFO_SET_COLLECTION_INVALID_TYPE->value,
            );
        }

        if (!isset($typeDefinition['valueType'])) {
            throw new Exception(
                "SetCollection type definition is missing required 'valueType' property",
                ExceptionCode::TYPEINFO_SET_COLLECTION_MISSING_VALUETYPE->value ,
                [
                    'provided_keys' => array_keys($typeDefinition),
                    'required_keys' => ['type', 'valueType'],
                ]
            );
        }

        $valueType = TypeFactory::getTypeInfoFromTypeDefinition($typeDefinition['valueType']);

        if (isset($typeDefinition['isFrozen']) && $typeDefinition['isFrozen'] === true) {
            $isFrozen = true;
        } else {
            $isFrozen = false;
        }

        return new self($valueType, $isFrozen);
    }
}
