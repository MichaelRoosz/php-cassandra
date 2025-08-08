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
     *  type: \Cassandra\Type::CUSTOM,
     *  javaClassName: string,
     * } $typeDefinition
     * 
     * @throws \Cassandra\TypeInfo\Exception
     */
    public static function fromTypeDefinition(array $typeDefinition): self {
        if (!isset($typeDefinition['type'])) {
            throw new Exception(
                "Custom type definition is missing required 'type' property",
                Exception::CUSTOM_MISSING_TYPE,
                [
                    'provided_keys' => array_keys($typeDefinition),
                    'required_keys' => ['type', 'javaClassName'],
                ]
            );
        }

        if ($typeDefinition['type'] !== Type::CUSTOM) {
            throw new Exception(
                "Invalid type definition for Custom: 'type' must be Type::CUSTOM",
                Exception::CUSTOM_INVALID_TYPE,
            );
        }

        if (!isset($typeDefinition['javaClassName'])) {
            throw new Exception(
                "Custom type definition is missing required 'javaClassName' property",
                Exception::CUSTOM_MISSING_JAVA_CLASS,
                [
                    'provided_keys' => array_keys($typeDefinition),
                    'required_keys' => ['type', 'javaClassName'],
                ]
            );
        }

        /** @psalm-suppress DocblockTypeContradiction */
        if (!is_string($typeDefinition['javaClassName'])) {
            throw new Exception(
                'Invalid type definition for Custom: javaClassName must be a string',
                Exception::CUSTOM_JAVA_CLASS_NOT_STRING,
                [
                    'javaClassName_type' => gettype($typeDefinition['javaClassName']),
                    'expected_type' => 'string',
                ]
            );
        }

        $javaClassName = $typeDefinition['javaClassName'];

        return new self($javaClassName);
    }
}
