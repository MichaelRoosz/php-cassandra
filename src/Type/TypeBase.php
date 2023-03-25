<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Stringable;

abstract class TypeBase implements Stringable {
    /**
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\Response\Exception
     */
    public function __toString(): string {
        /**
         *  @throws \Cassandra\Type\Exception
         *  @throws \Cassandra\Response\Exception
         * */
        $json = json_encode(
            $this->getValue(),
            JSON_PRETTY_PRINT
            | JSON_PRESERVE_ZERO_FRACTION
            | JSON_PARTIAL_OUTPUT_ON_ERROR
            | JSON_UNESCAPED_LINE_TERMINATORS
            | JSON_UNESCAPED_SLASHES
            | JSON_UNESCAPED_UNICODE
        );

        return $json === false ? '' : $json;
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    abstract public static function fromBinary(string $binary, null|int|array $definition = null): static;

    /**
     * @param mixed $value
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    abstract public static function fromValue(mixed $value, null|int|array $definition = null): static;

    abstract public function getBinary(): string;

    abstract public function getValue(): mixed;
}
