<?php

declare(strict_types=1);

namespace Cassandra\TypeInfo;

use Cassandra\ExceptionCode;
use Cassandra\Type;
use Cassandra\ValueFactory;

final class UDTInfo extends TypeInfo {
    public function __construct(
        /**
         * @var array<string,TypeInfo> $valueTypes
         */
        public readonly array $valueTypes,
        public readonly bool $isFrozen,
        public readonly ?string $keyspace = null,
        public readonly ?string $name = null,
    ) {
        parent::__construct(Type::UDT);
    }

    /**
     * @param array{
     *  type: \Cassandra\Type::UDT,
     *  valueTypes: array<string,\Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>)>,
     *  isFrozen: bool,
     *  keyspace?: string,
     *  name?: string,
     * } $typeDefinition
     * 
     * @throws \Cassandra\TypeInfo\Exception
     * @throws \Cassandra\Value\Exception
     */
    public static function fromTypeDefinition(array $typeDefinition): self {

        if (!isset($typeDefinition['type'])) {
            throw new Exception(
                "UDT type definition is missing required 'type' property",
                ExceptionCode::TYPEINFO_UDT_MISSING_TYPE->value,
                [
                    'provided_keys' => array_keys($typeDefinition),
                    'required_keys' => ['type', 'valueTypes'],
                ]
            );
        }

        if ($typeDefinition['type'] !== Type::UDT) {
            throw new Exception(
                "Invalid type definition for UDT: 'type' must be Type::UDT",
                ExceptionCode::TYPEINFO_UDT_INVALID_TYPE->value,
            );
        }

        if (!isset($typeDefinition['valueTypes'])) {
            throw new Exception(
                "UDT type definition is missing required 'valueTypes' property",
                ExceptionCode::TYPEINFO_UDT_MISSING_VALUETYPES->value,
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
                ExceptionCode::TYPEINFO_UDT_VALUETYPES_NOT_ARRAY->value,
                [
                    'valueTypes_type' => gettype($typeDefinition['valueTypes']),
                    'expected_type' => 'array',
                ]
            );
        }

        $valueTypes = [];
        foreach ($typeDefinition['valueTypes'] as $key => $valueTypeDefinition) {
            $valueTypes[$key] = ValueFactory::getTypeInfoFromTypeDefinition($valueTypeDefinition);
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

        if (isset($typeDefinition['isFrozen']) && $typeDefinition['isFrozen'] === true) {
            $isFrozen = true;
        } else {
            $isFrozen = false;
        }

        return new self($valueTypes, $isFrozen, $keyspace, $name);
    }
}
