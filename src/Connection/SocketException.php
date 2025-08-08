<?php

declare(strict_types=1);

namespace Cassandra\Connection;

final class SocketException extends NodeException {
    // code range 40000 to 49999
    public const CODE_CONNECT_FAILED = 40000;
    public const CODE_INVALID_CONFIG = 40001;
    public const CODE_NOT_CONNECTED_READ = 40002;
    public const CODE_NOT_CONNECTED_READ_ONCE = 40003;
    public const CODE_NOT_CONNECTED_WRITE = 40004;
    public const CODE_READ_FAILED = 40005;
    public const CODE_READ_FAILED_CONT = 40006;
    public const CODE_READ_NO_DATA = 40007;
    public const CODE_READ_NO_DATA_CONT = 40008;
    public const CODE_READ_ONCE_FAILED = 40009;
    public const CODE_READ_ONCE_NO_DATA = 40010;
    public const CODE_SOCKET_CREATE_FAILED = 40011;
    public const CODE_WRITE_FAILED = 40012;
}
