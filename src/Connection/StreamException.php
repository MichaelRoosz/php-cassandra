<?php

declare(strict_types=1);

namespace Cassandra\Connection;

final class StreamException extends NodeException {
    // Stream transport errors (1201-1299)
    public const CODE_INVALID_CONFIG = 1201;
    public const CODE_NOT_CONNECTED_READ = 1202;
    public const CODE_RESET_BY_PEER_READ = 1203;
    public const CODE_TIMEOUT_READ = 1204;
    public const CODE_READ_FAILED = 1205;
    public const CODE_NOT_CONNECTED_READ_ONCE = 1206;
    public const CODE_RESET_BY_PEER_READ_ONCE = 1207;
    public const CODE_TIMEOUT_READ_ONCE = 1208;
    public const CODE_READ_ONCE_FAILED = 1209;
    public const CODE_NOT_CONNECTED_WRITE = 1210;
    public const CODE_RESET_BY_PEER_WRITE = 1211;
    public const CODE_TIMEOUT_WRITE = 1212;
    public const CODE_WRITE_FAILED = 1213;
    public const CODE_CONNECT_FAILED = 1214;
}
