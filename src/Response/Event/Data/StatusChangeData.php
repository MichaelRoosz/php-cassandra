<?php

declare(strict_types=1);

namespace Cassandra\Response\Event\Data;

final class StatusChangeData extends EventData {
    public function __construct(
        public readonly StatusChangeType $changeType,
        public readonly string $address,
    ) {
        parent::__construct();
    }
}
