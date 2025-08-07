<?php

declare(strict_types=1);

namespace Cassandra\Response\Error\Context;

use Cassandra\Consistency;

final class UnavailableExceptionContext extends ErrorContext {
    public function __construct(
        public readonly Consistency $consistency,
        public readonly int $nodesRequired,
        public readonly int $nodesAlive,
    ) {
    }

    /**
     * @return array{
     *   consistency: int,
     *   nodes_required: int,
     *   nodes_alive: int,
     * }
     */
    #[\Override]
    public function toArray(): array {
        return [
            'consistency' => $this->consistency->value,
            'nodes_required' => $this->nodesRequired,
            'nodes_alive' => $this->nodesAlive,
        ];
    }
}
