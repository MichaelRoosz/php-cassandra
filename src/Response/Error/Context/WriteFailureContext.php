<?php

declare(strict_types=1);

namespace Cassandra\Response\Error\Context;

use Cassandra\Consistency;

final class WriteFailureContext extends ErrorContext {
    /**
     * @param array<string, int>|null $reasonMap
     */
    public function __construct(
        public readonly Consistency $consistency,
        public readonly int $nodesAnswered,
        public readonly int $nodesRequired,
        public readonly ?array $reasonMap,
        public readonly ?int $numFailures,
        public readonly string $writeType
    ) {
    }

    /**
     * @return array{
     *   consistency: int,
     *   nodes_answered: int,
     *   nodes_required: int,
     *   write_type: string,
     *   reasonmap: array<string, int>|null,
     *   num_failures: int|null,
     * }
     */
    #[\Override]
    public function toArray(): array {
        $data = [
            'consistency' => $this->consistency->value,
            'nodes_answered' => $this->nodesAnswered,
            'nodes_required' => $this->nodesRequired,
            'write_type' => $this->writeType,
            'reasonmap' => $this->reasonMap,
            'num_failures' => $this->numFailures,
        ];

        return $data;
    }
}
