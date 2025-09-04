<?php

declare(strict_types=1);

namespace Cassandra\Connection;

interface NodeSelector {
    /**
     * @param array<NodeConfig> $nodes
     * @return array<NodeConfig>
     */
    public function order(array $nodes): array;
}
