<?php

declare(strict_types=1);

namespace Cassandra\TypeInfo;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\TypeInfoException;
use Cassandra\Type;
use Cassandra\ValueFactory;

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
     *  @throws \Cassandra\Exception\TypeInfoException
     *  @throws \Cassandra\Exception\ValueFactoryException
     */
    public static function fromTypeDefinition(array $typeDefinition): self {
        if (!isset($typeDefinition['type'])) {
            throw new TypeInfoException(
                "Simple type definition is missing required 'type' property",
                ExceptionCode::TYPEINFO_SIMPLE_MISSING_TYPE->value,
                [
                    'provided_keys' => array_keys($typeDefinition),
                    'required_keys' => ['type'],
                ]
            );
        }

        if (!ValueFactory::isSimpleType($typeDefinition['type'])) {
            throw new TypeInfoException(
                'Invalid simple type definition: type must be a simple (non-complex) type',
                ExceptionCode::TYPEINFO_SIMPLE_NOT_SIMPLE_TYPE->value,
                [
                    'actual_type_value' => $typeDefinition['type']->value,
                    'actual_type_name' => $typeDefinition['type']->name,
                ]
            );
        }

        return new self($typeDefinition['type']);
    }
}
