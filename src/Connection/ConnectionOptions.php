<?php

declare(strict_types=1);

namespace Cassandra\Connection;

final class ConnectionOptions {
    public const PHP_CASSANDRA_DRIVER_NAME = 'php-cassandra-client';
    public const PHP_CASSANDRA_DRIVER_VERSION = '0.9.0';

    public function __construct(
        public readonly bool $enableCompression = false,
        public readonly bool $throwOnOverload = false,
        public readonly NodeSelectionStrategy $nodeSelectionStrategy = NodeSelectionStrategy::Random,
    ) {

    }

    /**
     * @return array<string,string>
     */
    public function toArray(): array {

        $options = [
            'CQL_VERSION' => '3.0.0',
            'DRIVER_NAME' => self::PHP_CASSANDRA_DRIVER_NAME,
            'DRIVER_VERSION' => self::PHP_CASSANDRA_DRIVER_VERSION,
        ];

        if ($this->enableCompression) {
            $options['COMPRESSION'] = 'lz4';
        }

        if ($this->throwOnOverload) {
            $options['THROW_ON_OVERLOAD'] = '1';
        }

        return $options;
    }
}
