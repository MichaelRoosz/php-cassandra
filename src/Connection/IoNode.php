<?php

declare(strict_types=1);

namespace Cassandra\Connection;

interface IoNode extends Node {
    /**
     * @throws \Cassandra\Exception\NodeException
     */
    public function __construct(NodeConfig $config);
}
