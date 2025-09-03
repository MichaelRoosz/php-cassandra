<?php

declare(strict_types=1);

namespace Cassandra;

enum StatementStatus: int {
    case AUTO_PREPARING = 200;
    case CREATED = 0;
    case REPREPARING = 300;
    case RESULT_READY = 700;
    case WAITING_FOR_RESULT = 100;
}
