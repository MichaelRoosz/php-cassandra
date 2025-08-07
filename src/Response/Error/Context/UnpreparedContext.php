<?php

declare(strict_types=1);

namespace Cassandra\Response\Error\Context;

final class UnpreparedContext extends ErrorContext {
    public function __construct(
        public readonly string $unknownStatementId
    ) {
    }

    /**
     * @return array{
     *   unknown_statement_id: string,
     * }
     */
    #[\Override]
    public function toArray(): array {
        return [
            'unknown_statement_id' => $this->unknownStatementId,
        ];
    }
}
