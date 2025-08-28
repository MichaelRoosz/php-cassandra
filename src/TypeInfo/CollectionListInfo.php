<?php

declare(strict_types=1);

namespace Cassandra\TypeInfo;

use Cassandra\ExceptionCode;
use Cassandra\Type;
use Cassandra\TypeFactory;

final class CollectionListInfo extends TypeInfo {
    public function __construct(
        public readonly TypeInfo $valueType,
        public readonly bool $isFrozen,
    ) {
        parent::__construct(Type::COLLECTION_LIST);
    }

    /**
     * @param array{
     *  type: \Cassandra\Type::COLLECTION_LIST,
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
                "CollectionList type definition is missing required 'type' property",
                ExceptionCode::TYPEINFO_COLLECTION_LIST_MISSING_TYPE->value,
                [
                    'provided_keys' => array_keys($typeDefinition),
                    'required_keys' => ['type', 'valueType'],
                ]
            );
        }

        if ($typeDefinition['type'] !== Type::COLLECTION_LIST) {
            throw new Exception(
                "Invalid type definition for CollectionList: 'type' must be Type::COLLECTION_LIST",
                ExceptionCode::TYPEINFO_COLLECTION_LIST_INVALID_TYPE->value,
            );
        }

        if (!isset($typeDefinition['valueType'])) {
            throw new Exception(
                "CollectionList type definition is missing required 'valueType' property",
                ExceptionCode::TYPEINFO_COLLECTION_LIST_MISSING_VALUETYPE->value,
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
