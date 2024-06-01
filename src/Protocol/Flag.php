<?php

declare(strict_types=1);

namespace Cassandra\Protocol;

interface Flag {
    final public const COMPRESSION = 0x01; // deprecated in v5
    final public const CUSTOM_PAYLOAD = 0x04;
    final public const TRACING = 0x02;
    final public const USE_BETA = 0x10;
    final public const WARNING = 0x08;
}
