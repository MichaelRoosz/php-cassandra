<?php

declare(strict_types=1);

namespace Cassandra\Response\Error\Context;

use Cassandra\Consistency;

final class WriteTimeoutContext extends ErrorContext {
    public function __construct(
        public readonly Consistency $consistency,
        public readonly int $nodesAcknowledged,
        public readonly int $nodesRequired,
        public readonly string $writeType,
        public readonly ?int $contentions,
    ) {
        parent::__construct();
    }

    /**
     * @return array{
     *   consistency: int,
     *   nodesAcknowledged: int,
     *   nodesRequired: int,
     *   writeType: string,
     *   contentions: int|null,
     * }
     */
    #[\Override]
    public function toArray(): array {
        return [
            'consistency' => $this->consistency->value,
            'nodesAcknowledged' => $this->nodesAcknowledged,
            'nodesRequired' => $this->nodesRequired,
            'writeType' => $this->writeType,
            'contentions' => $this->contentions,
        ];
    }
}
