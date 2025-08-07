<?php

declare(strict_types=1);

namespace Cassandra\Response\Error\Context;

use Cassandra\Consistency;

final class WriteTimeoutContext extends ErrorContext {
    public function __construct(
        public readonly Consistency $consistency,
        public readonly int $nodesAcknowledged,
        public readonly int $nodesRequired,
        public readonly WriteType $writeType,
        public readonly ?int $contentions,
    ) {
        parent::__construct();
    }

    /**
     * @return array{
     *   consistency: int,
     *   nodes_acknowledged: int,
     *   nodes_required: int,
     *   write_type: string,
     *   contentions: int|null,
     * }
     */
    #[\Override]
    public function toArray(): array {
        return [
            'consistency' => $this->consistency->value,
            'nodes_acknowledged' => $this->nodesAcknowledged,
            'nodes_required' => $this->nodesRequired,
            'write_type' => $this->writeType->value,
            'contentions' => $this->contentions,
        ];
    }
}
