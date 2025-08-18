<?php

declare(strict_types=1);

namespace Cassandra\TypeInfo;

use Cassandra\ExceptionCode;
use Cassandra\Type;
use Cassandra\TypeFactory;

final class CollectionSetInfo extends TypeInfo {
    public function __construct(
        public readonly TypeInfo $valueType,
    ) {
        parent::__construct(Type::COLLECTION_SET);
    }

    /**
     * @param array{
     *  type: \Cassandra\Type::COLLECTION_SET,
     *  valueType: \Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>),
     * } $typeDefinition
     * 
     * @throws \Cassandra\TypeInfo\Exception
     * @throws \Cassandra\Type\Exception
     */
    public static function fromTypeDefinition(array $typeDefinition): self {
        if (!isset($typeDefinition['type'])) {
            throw new Exception(
                "CollectionSet type definition is missing required 'type' property",
                ExceptionCode::TYPEINFO_COLLECTION_SET_MISSING_TYPE->value,
                [
                    'provided_keys' => array_keys($typeDefinition),
                    'required_keys' => ['type', 'valueType'],
                ]
            );
        }

        if ($typeDefinition['type'] !== Type::COLLECTION_SET) {
            throw new Exception(
                "Invalid type definition for CollectionSet: 'type' must be Type::COLLECTION_SET",
                ExceptionCode::TYPEINFO_COLLECTION_SET_INVALID_TYPE->value,
            );
        }

        if (!isset($typeDefinition['valueType'])) {
            throw new Exception(
                "CollectionSet type definition is missing required 'valueType' property",
                ExceptionCode::TYPEINFO_COLLECTION_SET_MISSING_VALUETYPE->value ,
                [
                    'provided_keys' => array_keys($typeDefinition),
                    'required_keys' => ['type', 'valueType'],
                ]
            );
        }

        $valueType = TypeFactory::getTypeInfoFromTypeDefinition($typeDefinition['valueType']);

        return new self($valueType);
    }
}
