<?php

declare(strict_types=1);

namespace Cassandra\Response\Event\Data;

enum StatusChangeType: string {
    case DOWN = 'DOWN';
    case UP = 'UP';
}
