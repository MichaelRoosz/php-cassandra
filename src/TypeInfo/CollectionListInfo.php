<?php

declare(strict_types=1);

namespace Cassandra\TypeInfo;

use Cassandra\Type;
use Cassandra\TypeFactory;

final class CollectionListInfo extends TypeInfo {
    public function __construct(
        public readonly TypeInfo $valueType,
    ) {
        parent::__construct(Type::COLLECTION_LIST);
    }

    /**
     * @param array{
     *  type: \Cassandra\Type::COLLECTION_LIST,
     *  valueType: \Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>),
     * } $typeDefinition
     *
     * @throws \Cassandra\TypeInfo\Exception
     * @throws \Cassandra\Type\Exception
     */
    public static function fromTypeDefinition(array $typeDefinition): self {
        if (!isset($typeDefinition['type'])) {
            throw new Exception(
                "CollectionList type definition is missing required 'type' property",
                Exception::COLLECTION_LIST_MISSING_TYPE,
                [
                    'provided_keys' => array_keys($typeDefinition),
                    'required_keys' => ['type', 'valueType'],
                ]
            );
        }

        if ($typeDefinition['type'] !== Type::COLLECTION_LIST) {
            throw new Exception(
                "Invalid type definition for CollectionList: 'type' must be Type::COLLECTION_LIST",
                Exception::COLLECTION_LIST_INVALID_TYPE,
                [
                    'actual_type_value' => $typeDefinition['type']->value,
                    'actual_type_name' => $typeDefinition['type']->name,
                    'expected_type_value' => Type::COLLECTION_LIST->value,
                    'expected_type_name' => Type::COLLECTION_LIST->name,
                ]
            );
        }

        if (!isset($typeDefinition['valueType'])) {
            throw new Exception(
                "CollectionList type definition is missing required 'valueType' property",
                Exception::COLLECTION_LIST_MISSING_VALUETYPE,
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
