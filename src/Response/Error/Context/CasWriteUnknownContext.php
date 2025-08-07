<?php

declare(strict_types=1);

namespace Cassandra\Response\Error\Context;

use Cassandra\Consistency;

final class CasWriteUnknownContext extends ErrorContext {
    public function __construct(
        public readonly Consistency $consistency,
        public readonly int $nodesAcknowledged,
        public readonly int $nodesRequired,
    ) {
    }

    /**
     * @return array{
     *   consistency: int,
     *   nodes_acknowledged: int,
     *   nodes_required: int,
     * }
     */
    #[\Override]
    public function toArray(): array {
        return [
            'consistency' => $this->consistency->value,
            'nodes_acknowledged' => $this->nodesAcknowledged,
            'nodes_required' => $this->nodesRequired,
        ];
    }
}
