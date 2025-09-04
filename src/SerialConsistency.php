<?php

declare(strict_types=1);

namespace Cassandra;

enum SerialConsistency: int {
    case LOCAL_SERIAL = 0x0009;
    case SERIAL = 0x0008;
}
