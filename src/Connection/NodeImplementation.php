<?php

declare(strict_types=1);

namespace Cassandra\Connection;

interface NodeImplementation extends Node {

    public function __construct(NodeConfig $config);

    public function write(string $binary): void;
}
