<?php

declare(strict_types=1);

namespace Cassandra\Response;

enum EventType: string {
    case SCHEMA_CHANGE = 'SCHEMA_CHANGE';
    case STATUS_CHANGE = 'STATUS_CHANGE';
    case TOPOLOGY_CHANGE = 'TOPOLOGY_CHANGE';
}
