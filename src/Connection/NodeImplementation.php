<?php

declare(strict_types=1);

namespace Cassandra\Connection;

interface NodeImplementation extends Node {
    /**
     * @throws \Cassandra\Connection\NodeException
     */
    public function __construct(NodeConfig $config);

    /**
     * @throws \Cassandra\Connection\NodeException
     */
    public function write(string $binary): void;
}
