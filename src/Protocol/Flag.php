<?php

declare(strict_types=1);

namespace Cassandra\Protocol;

enum Flag: int {
    case COMPRESSION = 0x01; // deprecated in v5
    case CUSTOM_PAYLOAD = 0x04;
    case TRACING = 0x02;
    case USE_BETA = 0x10;
    case WARNING = 0x08;
}
