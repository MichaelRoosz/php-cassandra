<?php

declare(strict_types=1);

namespace Cassandra\TypeInfo;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\TypeInfoException;
use Cassandra\Type;
use Cassandra\ValueFactory;

final class TupleInfo extends TypeInfo {
    /**
     * @param list<TypeInfo> $valueTypes
     */
    public function __construct(
        public readonly array $valueTypes,
    ) {
        parent::__construct(Type::TUPLE);
    }

    /**
     * @param array{
     *  type: \Cassandra\Type::TUPLE,
     *  valueTypes: list<\Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>)>,
     * } $typeDefinition
     * 
     * @throws \Cassandra\Exception\TypeInfoException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    public static function fromTypeDefinition(array $typeDefinition): self {

        if (!isset($typeDefinition['type'])) {
            throw new TypeInfoException(
                "Tuple type definition is missing required 'type' property",
                ExceptionCode::TYPEINFO_TUPLE_MISSING_TYPE->value,
                [
                    'provided_keys' => array_keys($typeDefinition),
                    'required_keys' => ['type', 'valueTypes'],
                ]
            );
        }

        if ($typeDefinition['type'] !== Type::TUPLE) {
            throw new TypeInfoException(
                "Invalid type definition for Tuple: 'type' must be Type::TUPLE",
                ExceptionCode::TYPEINFO_TUPLE_INVALID_TYPE->value,
            );
        }

        if (!isset($typeDefinition['valueTypes'])) {
            throw new TypeInfoException(
                "Tuple type definition is missing required 'valueTypes' property",
                ExceptionCode::TYPEINFO_TUPLE_MISSING_VALUETYPES->value,
                [
                    'provided_keys' => array_keys($typeDefinition),
                    'required_keys' => ['type', 'valueTypes'],
                ]
            );
        }

        /** @psalm-suppress DocblockTypeContradiction */
        if (!is_array($typeDefinition['valueTypes'])) {
            throw new TypeInfoException(
                "Invalid type definition for Tuple: 'valueTypes' must be an array",
                ExceptionCode::TYPEINFO_TUPLE_VALUETYPES_NOT_ARRAY->value,
                [
                    'valueTypes_type' => gettype($typeDefinition['valueTypes']),
                    'expected_type' => 'array',
                ]
            );
        }

        $valueTypes = [];
        foreach ($typeDefinition['valueTypes'] as $valueTypeDefinition) {
            $valueTypes[] = ValueFactory::getTypeInfoFromTypeDefinition($valueTypeDefinition);
        }

        return new self($valueTypes);
    }
}
