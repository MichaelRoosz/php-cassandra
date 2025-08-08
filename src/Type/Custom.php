<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\TypeInfo\CustomInfo;
use Cassandra\TypeInfo\TypeInfo;

/**
 * @api
 */
class Custom extends TypeBase {
    protected CustomInfo $typeInfo;

    protected string $value;

    final public function __construct(string $value, string $javaClassName) {

        $this->typeInfo = new CustomInfo($javaClassName);
        $this->value = $value;
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static {

        if ($typeInfo === null) {
            throw new Exception('typeInfo is required', Exception::CODE_CUSTOM_TYPEINFO_REQUIRED);
        }

        if (!$typeInfo instanceof CustomInfo) {
            throw new Exception('Invalid type info, CustomInfo expected', Exception::CODE_CUSTOM_INVALID_TYPEINFO, [
                'given_type' => get_class($typeInfo),
            ]);
        }

        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('n', substr($binary, 0, 2));
        if ($unpacked === false) {
            throw new Exception('Cannot unpack custom type binary header', Exception::CODE_CUSTOM_UNPACK_FAILED, [
                'binary_length' => strlen($binary),
                'expected_header_length' => 2,
            ]);
        }

        $length = $unpacked[1];

        return new static(substr($binary, 2, $length), $typeInfo->javaClassName);
    }

    /**
     * @param mixed $value
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {

        if ($typeInfo === null) {
            throw new Exception('typeInfo is required', Exception::CODE_CUSTOM_TYPEINFO_REQUIRED);
        }

        if (!$typeInfo instanceof CustomInfo) {
            throw new Exception('Invalid type info, CustomInfo expected', Exception::CODE_CUSTOM_INVALID_TYPEINFO, [
                'given_type' => get_class($typeInfo),
            ]);
        }

        if (!is_string($value)) {
            throw new Exception('Invalid custom value; expected string', Exception::CODE_CUSTOM_INVALID_VALUE_TYPE, [
                'value_type' => gettype($value),
            ]);
        }

        return new static($value, $typeInfo->javaClassName);
    }

    #[\Override]
    public function getBinary(): string {
        return pack('n', strlen($this->value)) . $this->value;
    }

    public function getJavaClassName(): string {
        return $this->typeInfo->javaClassName;
    }

    #[\Override]
    public function getValue(): string {
        return $this->value;
    }
}
