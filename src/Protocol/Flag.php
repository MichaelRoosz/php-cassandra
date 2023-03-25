<?php

declare(strict_types=1);

namespace Cassandra\Protocol;

interface Flag {
    public final const COMPRESSION = 0x01; // deprecated in v5
    public final const CUSTOM_PAYLOAD = 0x04;
    public final const TRACING = 0x02;
    public final const USE_BETA = 0x10;
    public final const WARNING = 0x08;
}
