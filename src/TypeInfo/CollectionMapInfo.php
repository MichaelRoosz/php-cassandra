<?php

declare(strict_types=1);

namespace Cassandra\TypeInfo;

use Cassandra\Type;
use Cassandra\TypeFactory;

final class CollectionMapInfo extends TypeInfo {
    public function __construct(
        public readonly TypeInfo $keyType,
        public readonly TypeInfo $valueType,
    ) {
        parent::__construct(Type::COLLECTION_MAP);
    }

    /**
     * @param array{
     *  type: Type::COLLECTION_MAP,
     *  keyType: Type|array<mixed>,
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

        if ($typeDefinition['type'] !== Type::COLLECTION_MAP) {
            throw new Exception('Invalid type definition, must be a CollectionMap');
        }

        if (!isset($typeDefinition['keyType'])) {
            throw new Exception('Type definition must have a keyType property');
        }
        $keyType = TypeFactory::getTypeInfoFromTypeDefinition($typeDefinition['keyType']);

        if (!isset($typeDefinition['valueType'])) {
            throw new Exception('Type definition must have a valueType property');
        }
        $valueType = TypeFactory::getTypeInfoFromTypeDefinition($typeDefinition['valueType']);

        return new self($keyType, $valueType);
    }

}
