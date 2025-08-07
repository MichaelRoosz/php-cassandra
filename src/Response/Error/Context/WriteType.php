<?php

declare(strict_types=1);

namespace Cassandra\Response\Error\Context;

enum WriteType: string {
    case BATCH = 'BATCH';
    case BATCH_LOG = 'BATCH_LOG';
    case CAS = 'CAS';
    case CDC = 'CDC';
    case COUNTER = 'COUNTER';
    case SIMPLE = 'SIMPLE';
    case UNLOGGED_BATCH = 'UNLOGGED_BATCH';
    case VIEW = 'VIEW';
}
