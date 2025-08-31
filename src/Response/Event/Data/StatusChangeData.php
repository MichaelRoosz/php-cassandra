<?php

declare(strict_types=1);

namespace Cassandra\Response\Event\Data;

final class StatusChangeData extends EventData {
    public function __construct(
        public readonly StatusChangeType $changeType,

        /** @var array{ip: string, port: int} $address */
        public readonly array $address,
    ) {
        parent::__construct();
    }
}
