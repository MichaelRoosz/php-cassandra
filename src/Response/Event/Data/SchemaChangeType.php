<?php

declare(strict_types=1);

namespace Cassandra\Response\Event\Data;

enum SchemaChangeType: string {
    case CREATED = 'CREATED';
    case DROPPED = 'DROPPED';
    case UPDATED = 'UPDATED';
}
