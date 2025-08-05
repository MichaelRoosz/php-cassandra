<?php

declare(strict_types=1);

namespace Cassandra\Response\Event\Data;

final class TopologyChangeData extends EventData {
    public function __construct(
        public readonly TopologyChangeType $changeType,
        public readonly string $address,
    ) {
        parent::__construct();
    }
}
