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
     *  type: Type::COLLECTION_LIST,
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

        if ($typeDefinition['type'] !== Type::COLLECTION_LIST) {
            throw new Exception('Invalid type definition, must be a CollectionList');
        }

        if (!isset($typeDefinition['valueType'])) {
            throw new Exception('Type definition must have a valueType property');
        }

        $valueType = TypeFactory::getTypeInfoFromTypeDefinition($typeDefinition['valueType']);

        return new self($valueType);
    }
}
