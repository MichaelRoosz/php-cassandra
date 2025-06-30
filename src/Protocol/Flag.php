<?php

declare(strict_types=1);

namespace Cassandra\Protocol;

interface Flag {
    public const COMPRESSION = 0x01; // deprecated in v5
    public const CUSTOM_PAYLOAD = 0x04;
    public const TRACING = 0x02;
    public const USE_BETA = 0x10;
    public const WARNING = 0x08;
}
