<?php

declare(strict_types=1);

namespace Cassandra\TypeInfo;

use Cassandra\Type;
use Cassandra\TypeFactory;

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
     *  type: Type::TUPLE,
     *  valueTypes: list<Type|(array{ type: Type }&array<mixed>)>,
     * } $typeDefinition
     * 
     * @throws \Cassandra\TypeInfo\Exception
     * @throws \Cassandra\Type\Exception
     */
    public static function fromTypeDefinition(array $typeDefinition): self {

        if (!isset($typeDefinition['type'])) {
            throw new Exception('Tuple type definition must have a type property');
        }

        if ($typeDefinition['type'] !== Type::TUPLE) {
            throw new Exception('Invalid type definition, must be a Tuple');
        }

        if (!isset($typeDefinition['valueTypes'])) {
            throw new Exception('Tuple type definition must have a valueTypes property');
        }

        /** @psalm-suppress DocblockTypeContradiction */
        if (!is_array($typeDefinition['valueTypes'])) {
            throw new Exception('Invalid type definition, valueTypes must be an array');
        }

        $valueTypes = [];
        foreach ($typeDefinition['valueTypes'] as $valueTypeDefinition) {
            $valueTypes[] = TypeFactory::getTypeInfoFromTypeDefinition($valueTypeDefinition);
        }

        return new self($valueTypes);
    }
}
