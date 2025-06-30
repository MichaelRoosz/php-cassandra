<?php

declare(strict_types=1);

namespace Cassandra;

enum Consistency: int {
    case ALL = 0x0005;
    case ANY = 0x0000;
    case EACH_QUORUM = 0x0007;
    case LOCAL_ONE = 0x000A;
    case LOCAL_QUORUM = 0x0006;
    case LOCAL_SERIAL = 0x0009;
    case ONE = 0x0001;
    case QUORUM = 0x0004;
    case SERIAL = 0x0008;
    case THREE = 0x0003;
    case TWO = 0x0002;
}
