<?php

declare(strict_types=1);

namespace Cassandra\TypeInfo;

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
     *  type: Type::COLLECTION_SET,
     *  valueType: Type|array<mixed>,
     * } $typeDefinition
     * 
     * @throws \Cassandra\TypeInfo\Exception
     * @throws \Cassandra\Type\Exception
     */
    public static function fromTypeDefinition(array $typeDefinition): self {
        if (!isset($typeDefinition['type'])) {
            throw new Exception('Type definition must have a type property');
        }

        if ($typeDefinition['type'] !== Type::COLLECTION_SET) {
            throw new Exception('Invalid type definition, must be a CollectionSet');
        }

        if (!isset($typeDefinition['valueType'])) {
            throw new Exception('Type definition must have a valueType property');
        }

        $valueType = TypeFactory::getTypeInfoFromTypeDefinition($typeDefinition['valueType']);

        return new self($valueType);
    }
}
