<?php

declare(strict_types=1);

namespace Cassandra\TypeInfo;

use Cassandra\Type;
use Cassandra\TypeFactory;

final class SimpleTypeInfo extends TypeInfo {
    public function __construct(
        Type $type,
    ) {
        parent::__construct($type);
    }

    /**
     * @param array{
     *  type: \Cassandra\Type,
     * } $typeDefinition
     * 
     * @throws \Cassandra\TypeInfo\Exception
     */
    public static function fromTypeDefinition(array $typeDefinition): self {
        if (!isset($typeDefinition['type'])) {
            throw new Exception('Type definition must have a type property');
        }

        if (!TypeFactory::isSimpleType($typeDefinition['type'])) {
            throw new Exception('Type must be a simple type');
        }

        return new self($typeDefinition['type']);
    }
}
