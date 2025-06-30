<?php

declare(strict_types=1);

namespace Cassandra\Request;

enum BatchType: int {
    case COUNTER = 2;
    case LOGGED = 0;
    case UNLOGGED = 1;
}
