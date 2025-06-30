<?php

declare(strict_types=1);

namespace Cassandra\Request;

enum PrepareFlag: int {
    case WITH_KEYSPACE = 0x01;
}
