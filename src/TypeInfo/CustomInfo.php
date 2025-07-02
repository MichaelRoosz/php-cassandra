<?php

declare(strict_types=1);

namespace Cassandra\TypeInfo;

use Cassandra\Type;

final class CustomInfo extends TypeInfo {
    public function __construct(
        public readonly string $javaClassName,
    ) {
        parent::__construct(Type::CUSTOM);
    }

    /**
     * @param array{
     *  type: Type::CUSTOM,
     *  javaClassName: string,
     * } $typeDefinition
     * 
     * @throws \Cassandra\TypeInfo\Exception
     */
    public static function fromTypeDefinition(array $typeDefinition): self {
        if (!isset($typeDefinition['type'])) {
            throw new Exception('Custom type definition must have a type property');
        }

        if ($typeDefinition['type'] !== Type::CUSTOM) {
            throw new Exception('Invalid type definition, must be a Custom');
        }

        if (!isset($typeDefinition['javaClassName'])) {
            throw new Exception('Type definition must have a javaClassName property');
        }

        /** @psalm-suppress DocblockTypeContradiction */
        if (!is_string($typeDefinition['javaClassName'])) {
            throw new Exception('Invalid type definition, javaClassName must be a string');
        }

        $javaClassName = $typeDefinition['javaClassName'];

        return new self($javaClassName);
    }
}
