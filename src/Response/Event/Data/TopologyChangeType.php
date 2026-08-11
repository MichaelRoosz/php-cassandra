<?php

declare(strict_types=1);

namespace Cassandra\Response\Event\Data;

enum TopologyChangeType: string {
    case MOVED_NODE = 'MOVED_NODE'; // Protocol-v3 topology event; removed from later protocol versions.
    case NEW_NODE = 'NEW_NODE';
    case REMOVED_NODE = 'REMOVED_NODE';
}
