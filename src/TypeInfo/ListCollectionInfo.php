<?php

declare(strict_types=1);

namespace Cassandra\TypeInfo;

use Cassandra\ExceptionCode;
use Cassandra\Type;
use Cassandra\ValueFactory;

final class ListCollectionInfo extends TypeInfo {
    public function __construct(
        public readonly TypeInfo $valueType,
        public readonly bool $isFrozen,
    ) {
        parent::__construct(Type::LIST);
    }

    /**
     * @param array{
     *  type: \Cassandra\Type::LIST,
     *  valueType: \Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>),
     *  isFrozen: bool,
     * } $typeDefinition
     *
     * @throws \Cassandra\TypeInfo\Exception
     * @throws \Cassandra\Value\Exception
     */
    public static function fromTypeDefinition(array $typeDefinition): self {
        if (!isset($typeDefinition['type'])) {
            throw new Exception(
                "ListCollection type definition is missing required 'type' property",
                ExceptionCode::TYPEINFO_LIST_MISSING_TYPE->value,
                [
                    'provided_keys' => array_keys($typeDefinition),
                    'required_keys' => ['type', 'valueType'],
                ]
            );
        }

        if ($typeDefinition['type'] !== Type::LIST) {
            throw new Exception(
                "Invalid type definition for ListCollection: 'type' must be Type::LIST",
                ExceptionCode::TYPEINFO_LIST_INVALID_TYPE->value,
            );
        }

        if (!isset($typeDefinition['valueType'])) {
            throw new Exception(
                "ListCollection type definition is missing required 'valueType' property",
                ExceptionCode::TYPEINFO_LIST_MISSING_VALUETYPE->value,
                [
                    'provided_keys' => array_keys($typeDefinition),
                    'required_keys' => ['type', 'valueType'],
                ]
            );
        }

        $valueType = ValueFactory::getTypeInfoFromTypeDefinition($typeDefinition['valueType']);

        if (isset($typeDefinition['isFrozen']) && $typeDefinition['isFrozen'] === true) {
            $isFrozen = true;
        } else {
            $isFrozen = false;
        }

        return new self($valueType, $isFrozen);
    }
}
