<?php

declare(strict_types=1);

namespace Cassandra\Response\Error\Context;

final class AlreadyExistsContext extends ErrorContext {
    public function __construct(
        public readonly string $keyspace,
        public readonly string $table,
    ) {
    }

    /**
     * @return array{
     *   keyspace: string,
     *   table: string,
     * }
     */
    #[\Override]
    public function toArray(): array {
        return [
            'keyspace' => $this->keyspace,
            'table' => $this->table,
        ];
    }
}
