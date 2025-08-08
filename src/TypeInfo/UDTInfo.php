<?php

declare(strict_types=1);

namespace Cassandra\TypeInfo;

use Cassandra\Type;
use Cassandra\TypeFactory;

final class UDTInfo extends TypeInfo {
    public function __construct(
        /**
         * @var array<string,TypeInfo> $valueTypes
         */
        public readonly array $valueTypes,
        public readonly ?string $keyspace = null,
        public readonly ?string $name = null,
    ) {
        parent::__construct(Type::UDT);
    }

    /**
     * @param array{
     *  type: \Cassandra\Type::UDT,
     *  valueTypes: array<string,\Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>)>,
     *  keyspace?: string,
     *  name?: string,
     * } $typeDefinition
     * 
     * @throws \Cassandra\TypeInfo\Exception
     * @throws \Cassandra\Type\Exception
     */
    public static function fromTypeDefinition(array $typeDefinition): self {

        if (!isset($typeDefinition['type'])) {
            throw new Exception(
                "UDT type definition is missing required 'type' property",
                Exception::UDT_MISSING_TYPE,
                [
                    'provided_keys' => array_keys($typeDefinition),
                    'required_keys' => ['type', 'valueTypes'],
                ]
            );
        }

        if ($typeDefinition['type'] !== Type::UDT) {
            throw new Exception(
                "Invalid type definition for UDT: 'type' must be Type::UDT",
                Exception::UDT_INVALID_TYPE,
                [
                    'actual_type_value' => $typeDefinition['type']->value,
                    'actual_type_name' => $typeDefinition['type']->name,
                    'expected_type_value' => Type::UDT->value,
                    'expected_type_name' => Type::UDT->name,
                ]
            );
        }

        if (!isset($typeDefinition['valueTypes'])) {
            throw new Exception(
                "UDT type definition is missing required 'valueTypes' property",
                Exception::UDT_MISSING_VALUETYPES,
                [
                    'provided_keys' => array_keys($typeDefinition),
                    'required_keys' => ['type', 'valueTypes'],
                ]
            );
        }

        /** @psalm-suppress DocblockTypeContradiction */
        if (!is_array($typeDefinition['valueTypes'])) {
            throw new Exception(
                "Invalid type definition for UDT: 'valueTypes' must be an array",
                Exception::UDT_VALUETYPES_NOT_ARRAY,
                [
                    'valueTypes_type' => gettype($typeDefinition['valueTypes']),
                    'expected_type' => 'array',
                ]
            );
        }

        $valueTypes = [];
        foreach ($typeDefinition['valueTypes'] as $key => $valueTypeDefinition) {
            $valueTypes[$key] = TypeFactory::getTypeInfoFromTypeDefinition($valueTypeDefinition);
        }

        /** @psalm-suppress RedundantConditionGivenDocblockType */
        if (isset($typeDefinition['keyspace']) && is_string($typeDefinition['keyspace'])) {
            $keyspace = $typeDefinition['keyspace'];
        } else {
            $keyspace = null;
        }

        /** @psalm-suppress RedundantConditionGivenDocblockType */
        if (isset($typeDefinition['name']) && is_string($typeDefinition['name'])) {
            $name = $typeDefinition['name'];
        } else {
            $name = null;
        }

        return new self($valueTypes, $keyspace, $name);
    }
}
