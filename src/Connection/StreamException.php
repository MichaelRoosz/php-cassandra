<?php

declare(strict_types=1);

namespace Cassandra\Connection;

final class StreamException extends NodeException {
    // code range 50000 to 59999
    public const CODE_CONNECT_FAILED = 50000;
    public const CODE_INVALID_CONFIG = 50001;
    public const CODE_NOT_CONNECTED_READ = 50002;
    public const CODE_NOT_CONNECTED_READ_ONCE = 50003;
    public const CODE_NOT_CONNECTED_WRITE = 50004;
    public const CODE_READ_FAILED = 50005;
    public const CODE_READ_ONCE_FAILED = 50006;
    public const CODE_RESET_BY_PEER_READ = 50007;
    public const CODE_RESET_BY_PEER_READ_ONCE = 50008;
    public const CODE_RESET_BY_PEER_WRITE = 50009;
    public const CODE_TIMEOUT_READ = 50010;
    public const CODE_TIMEOUT_READ_ONCE = 50011;
    public const CODE_TIMEOUT_WRITE = 50012;
    public const CODE_WRITE_FAILED = 50013;
}
