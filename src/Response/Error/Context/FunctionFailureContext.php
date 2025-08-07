<?php

declare(strict_types=1);

namespace Cassandra\Response\Error\Context;

final class FunctionFailureContext extends ErrorContext {
    /**
     * @param array<string> $argTypes
     */
    public function __construct(
        public readonly string $keyspace,
        public readonly string $function,
        public readonly array $argTypes,
    ) {
    }

    /**
     * @return array{
     *   keyspace: string,
     *   function: string,
     *   arg_types: array<string>,
     * }
     */
    #[\Override]
    public function toArray(): array {
        return [
            'keyspace' => $this->keyspace,
            'function' => $this->function,
            'arg_types' => $this->argTypes,
        ];
    }
}
