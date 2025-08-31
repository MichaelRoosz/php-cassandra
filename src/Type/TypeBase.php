<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\Response\StreamReader;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;
use Stringable;

abstract class TypeBase implements Stringable {
    /**
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\Response\Exception
     */
    #[\Override]
    public function __toString(): string {
        /** @psalm-suppress MixedAssignment */
        $value = $this->getValue();

        if (is_string($value)) {
            return $value;
        }

        /**
         *  @throws \Cassandra\Type\Exception
         *  @throws \Cassandra\Response\Exception
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
     * @throws \Cassandra\Type\Exception
     */
    abstract public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static;

    /**
     * @param mixed $value
     *
     * @throws \Cassandra\Type\Exception
     */
    abstract public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static;

    /**
     * @throws \Cassandra\Type\Exception
     */
    abstract public static function fromStream(StreamReader $stream, ?int $length = null, ?TypeInfo $typeInfo = null): static;

    abstract public function getBinary(): string;

    abstract public function getType(): Type;

    abstract public function getValue(): mixed;

    abstract public static function hasFixedLength(): bool;

    abstract public static function isReadableWithoutLength(): bool;

    abstract public static function isSerializedAsFixedLength(): bool;

    abstract public static function requiresDefinition(): bool;
}
