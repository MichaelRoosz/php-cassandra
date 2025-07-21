<?php

declare(strict_types=1);

namespace Cassandra\Connection;

abstract class NodeConfig {

    public function __construct(
        public readonly string $host = 'localhost',
        public readonly int $port = 9042,
        public readonly string $username = '',
        public readonly string $password = '',
    )
    {
        
    }

    /**
     * @return class-string<NodeImplementation>
     */
    abstract public function getNodeClass(): string;
}
