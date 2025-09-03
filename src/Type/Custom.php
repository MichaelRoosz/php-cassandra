<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\ExceptionCode;
use Cassandra\Type;
use Cassandra\TypeInfo\CustomInfo;
use Cassandra\TypeInfo\TypeInfo;

/**
 * @api
 */
class Custom extends TypeReadableWithLength {
    protected CustomInfo $typeInfo;

    protected readonly string $value;

    final public function __construct(string $value, CustomInfo $typeInfo) {

        $this->typeInfo = $typeInfo;
        $this->value = $value;
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static {

        if ($typeInfo === null) {
            throw new Exception('typeInfo is required', ExceptionCode::TYPE_CUSTOM_TYPEINFO_REQUIRED->value);
        }

        if (!$typeInfo instanceof CustomInfo) {
            throw new Exception('Invalid type info, CustomInfo expected', ExceptionCode::TYPE_CUSTOM_INVALID_TYPEINFO->value, [
                'given_type' => get_class($typeInfo),
            ]);
        }

        return new static($binary, $typeInfo);
    }

    /**
     * @param mixed $value
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {

        if ($typeInfo === null) {
            throw new Exception('typeInfo is required', ExceptionCode::TYPE_CUSTOM_TYPEINFO_REQUIRED->value);
        }

        if (!$typeInfo instanceof CustomInfo) {
            throw new Exception('Invalid type info, CustomInfo expected', ExceptionCode::TYPE_CUSTOM_INVALID_TYPEINFO->value, [
                'given_type' => get_class($typeInfo),
            ]);
        }

        if (!is_string($value)) {
            throw new Exception('Invalid custom value; expected string', ExceptionCode::TYPE_CUSTOM_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        return new static($value, $typeInfo);
    }

    /**
     * @throws \Cassandra\TypeInfo\Exception
     */
    final public static function fromValue(string $value, string $javaClassName): static {

        return new static($value, CustomInfo::fromTypeDefinition([
            'type' => Type::CUSTOM,
            'javaClassName' => $javaClassName,
        ]));
    }

    #[\Override]
    public function getBinary(): string {
        return $this->value;
    }

    public function getJavaClassName(): string {
        return $this->typeInfo->javaClassName;
    }

    #[\Override]
    public function getType(): Type {
        return Type::CUSTOM;
    }

    #[\Override]
    public function getValue(): string {
        return $this->value;
    }

    #[\Override]
    final public static function requiresDefinition(): bool {
        return false;
    }
}
