<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Protocol\ProtocolVersion;
use Cassandra\ReleaseConstants;

final class ConnectionOptions {
    public function __construct(
        public readonly bool $enableCompression = false,
        public readonly bool $throwOnOverload = false,
        public readonly NodeSelectionStrategy $nodeSelectionStrategy = NodeSelectionStrategy::Random,
        public readonly int $preparedResultCacheSize = 100,

        /** @var ProtocolVersion[] $allowedProtocolVersions */
        public readonly array $allowedProtocolVersions = ProtocolVersion::PREFRED_ORDER,
        public readonly ProtocolVersion $initialProtocolVersion = ProtocolVersion::V4,
    ) {
    }

    /**
     * @return array<string,string>
     */
    public function asStartupOptions(): array {

        $options = [
            'CQL_VERSION' => '3.0.0',
            'DRIVER_NAME' => ReleaseConstants::PHP_CASSANDRA_DRIVER_NAME,
            'DRIVER_VERSION' => ReleaseConstants::PHP_CASSANDRA_DRIVER_VERSION,
        ];

        if ($this->enableCompression) {
            $options['COMPRESSION'] = 'lz4';
        }

        if ($this->throwOnOverload) {
            $options['THROW_ON_OVERLOAD'] = '1';
        }

        return $options;
    }

    /**
     * @deprecated Use asStartupOptions() instead.
     * @return array<string,string>
     */
    public function toArray(): array {
        return $this->asStartupOptions();
    }
}
