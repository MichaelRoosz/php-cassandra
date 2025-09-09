<?php

declare(strict_types=1);

namespace Cassandra\TypeInfo;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\TypeInfoException;
use Cassandra\Type;
use Cassandra\ValueFactory;

final class VectorInfo extends TypeInfo {
    public function __construct(
        public readonly TypeInfo $valueType,
        public readonly int $dimensions,
    ) {
        parent::__construct(Type::VECTOR);
    }

    /**
     * @param array{
     *  type: \Cassandra\Type::VECTOR,
     *  valueType: \Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>),
     *  dimensions: int,
     * } $typeDefinition
     *
     * @throws \Cassandra\Exception\TypeInfoException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    public static function fromTypeDefinition(array $typeDefinition): self {
        if (!isset($typeDefinition['type'])) {
            throw new TypeInfoException(
                "Vector type definition is missing required 'type' property",
                ExceptionCode::TYPEINFO_VECTOR_MISSING_TYPE->value,
                [
                    'provided_keys' => array_keys($typeDefinition),
                    'required_keys' => ['type', 'valueType'],
                ]
            );
        }

        if ($typeDefinition['type'] !== Type::VECTOR) {
            throw new TypeInfoException(
                "Invalid type definition for Vector: 'type' must be Type::VECTOR",
                ExceptionCode::TYPEINFO_VECTOR_INVALID_TYPE->value,
            );
        }

        if (!isset($typeDefinition['valueType'])) {
            throw new TypeInfoException(
                "Vector type definition is missing required 'valueType' property",
                ExceptionCode::TYPEINFO_VECTOR_MISSING_VALUETYPE->value,
                [
                    'provided_keys' => array_keys($typeDefinition),
                    'required_keys' => ['type', 'valueType'],
                ]
            );
        }

        $valueType = ValueFactory::getTypeInfoFromTypeDefinition($typeDefinition['valueType']);

        if (!isset($typeDefinition['dimensions'])) {
            throw new TypeInfoException(
                "Vector type definition is missing required 'dimensions' property",
                ExceptionCode::TYPEINFO_VECTOR_MISSING_DIMENSIONS->value,
                [
                    'provided_keys' => array_keys($typeDefinition),
                    'required_keys' => ['type', 'dimensions'],
                ]
            );
        }

        /** @psalm-suppress DocblockTypeContradiction */
        if (!is_int($typeDefinition['dimensions'])) {
            throw new TypeInfoException(
                "Vector type definition 'dimensions' must be an integer",
                ExceptionCode::TYPEINFO_VECTOR_INVALID_DIMENSIONS->value,
            );
        }

        $dimensions = $typeDefinition['dimensions'];

        return new self($valueType, $dimensions);
    }
}
