<?php

declare(strict_types=1);

namespace Cassandra;

use Cassandra\Protocol\ProtocolVersion;

final class ReleaseConstants {
    public const PHP_CASSANDRA_DRIVER_NAME = 'php-cassandra-client';
    public const PHP_CASSANDRA_DRIVER_VERSION = '1.2.0';
    public const PHP_CASSANDRA_SUPPORTED_PROTOCOL_VERSIONS = ProtocolVersion::CASES_IN_OPTION_FORMAT;
}
