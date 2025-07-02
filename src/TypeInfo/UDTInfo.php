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
            throw new Exception('UDT type definition must have a type property');
        }

        if ($typeDefinition['type'] !== Type::UDT) {
            throw new Exception('Invalid type definition, must be a UDT');
        }

        if (!isset($typeDefinition['valueTypes'])) {
            throw new Exception('UDT type definition must have a valueTypes property');
        }

        /** @psalm-suppress DocblockTypeContradiction */
        if (!is_array($typeDefinition['valueTypes'])) {
            throw new Exception('Invalid type definition, valueTypes must be an array');
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
