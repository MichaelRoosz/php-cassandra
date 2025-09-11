<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\Response\StreamReader;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;
use Stringable;

abstract class ValueBase implements Stringable {
    /**
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    public function __toString(): string {
        /** @psalm-suppress MixedAssignment */
        $value = $this->getValue();

        if (is_string($value)) {
            return $value;
        }

        /**
         * @throws \Cassandra\Exception\ValueException
         * */
        $json = json_encode(
            $value,
            JSON_PRETTY_PRINT
            | JSON_PRESERVE_ZERO_FRACTION
            | JSON_PARTIAL_OUTPUT_ON_ERROR
            | JSON_UNESCAPED_LINE_TERMINATORS
            | JSON_UNESCAPED_SLASHES
            | JSON_UNESCAPED_UNICODE
        );

        return $json === false ? '' : $json;
    }

    abstract public static function fixedLength(): int;

    /**
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    abstract public static function fromBinary(
        string $binary,
        ?TypeInfo $typeInfo = null,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): static;

    /**
     * @param mixed $value
     *
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    abstract public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static;

    /**
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    abstract public static function fromStream(
        StreamReader $stream,
        ?int $length = null,
        ?TypeInfo $typeInfo = null,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): static;

    abstract public function getBinary(): string;

    abstract public function getType(): Type;

    abstract public function getValue(): mixed;

    abstract public static function hasFixedLength(): bool;

    abstract public static function isReadableWithoutLength(): bool;

    abstract public static function isSerializedAsFixedLength(): bool;

    abstract public static function requiresDefinition(): bool;

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    protected static function require64Bit(): void {
        if (PHP_INT_SIZE < 8) {
            $className = static::class;

            throw new ValueException('The ' . $className . ' data type requires 64-bit integers, 64-bit php is required', ExceptionCode::VALUE_TYPE_REQUIRES_64BIT_INTEGER->value, [
                'class' => $className,
                'php_int_size_bytes' => PHP_INT_SIZE,
                'php_int_size_bits' => PHP_INT_SIZE * 8,
            ]);
        }
    }
}
