<?php

declare(strict_types=1);

namespace Cassandra\Response\Event\Data;

final class TopologyChangeData extends EventData {
    public function __construct(
        public readonly TopologyChangeType $changeType,

        /** @var array{ip: string, port: int} $address */
        public readonly array $address,
    ) {
        parent::__construct();
    }
}
