<?php

declare(strict_types=1);

namespace Cassandra\Response\Event\Data;

enum TopologyChangeType: string {
    case NEW_NODE = 'NEW_NODE';
    case REMOVED_NODE = 'REMOVED_NODE';
}
