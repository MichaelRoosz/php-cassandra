<?php

declare(strict_types=1);

namespace Cassandra\Connection;

final class SocketException extends NodeException {
    public const CODE_CONNECT_FAILED = 1313;
    // Socket transport errors (1301-1399)
    public const CODE_INVALID_CONFIG = 1301;
    public const CODE_NOT_CONNECTED_READ = 1302;
    public const CODE_NOT_CONNECTED_READ_ONCE = 1307;
    public const CODE_NOT_CONNECTED_WRITE = 1310;
    public const CODE_READ_FAILED = 1303;
    public const CODE_READ_FAILED_CONT = 1305;
    public const CODE_READ_NO_DATA = 1304;
    public const CODE_READ_NO_DATA_CONT = 1306;
    public const CODE_READ_ONCE_FAILED = 1308;
    public const CODE_READ_ONCE_NO_DATA = 1309;
    public const CODE_SOCKET_CREATE_FAILED = 1312;
    public const CODE_WRITE_FAILED = 1311;
}
