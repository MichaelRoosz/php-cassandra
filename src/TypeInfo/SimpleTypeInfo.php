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
            throw new Exception(
                "Simple type definition is missing required 'type' property",
                Exception::SIMPLE_MISSING_TYPE,
                [
                    'provided_keys' => array_keys($typeDefinition),
                    'required_keys' => ['type'],
                ]
            );
        }

        if (!TypeFactory::isSimpleType($typeDefinition['type'])) {
            throw new Exception(
                'Invalid simple type definition: type must be a simple (non-complex) type',
                Exception::SIMPLE_NOT_SIMPLE_TYPE,
                [
                    'actual_type_value' => $typeDefinition['type']->value,
                    'actual_type_name' => $typeDefinition['type']->name,
                ]
            );
        }

        return new self($typeDefinition['type']);
    }
}
