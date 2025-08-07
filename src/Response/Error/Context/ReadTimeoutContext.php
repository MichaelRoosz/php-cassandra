<?php

declare(strict_types=1);

namespace Cassandra\Response\Error\Context;

use Cassandra\Consistency;

final class ReadTimeoutContext extends ErrorContext {
    public function __construct(
        public readonly Consistency $consistency,
        public readonly int $nodesAnswered,
        public readonly int $nodesRequired,
        public readonly bool $dataPresent
    ) {
    }

    /**
     * @return array{
     *   consistency: int,
     *   nodes_answered: int,
     *   nodes_required: int,
     *   data_present: bool,
     * }
     */
    #[\Override]
    public function toArray(): array {
        return [
            'consistency' => $this->consistency->value,
            'nodes_answered' => $this->nodesAnswered,
            'nodes_required' => $this->nodesRequired,
            'data_present' => $this->dataPresent,
        ];
    }
}
