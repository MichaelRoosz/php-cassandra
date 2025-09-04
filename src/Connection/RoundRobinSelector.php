<?php

declare(strict_types=1);

namespace Cassandra\Connection;

final class RoundRobinSelector implements NodeSelector {
    private int $counter = 0;

    #[\Override]
    public function order(array $nodes): array {
        $count = count($nodes);
        if ($count <= 1) {
            return $nodes;
        }

        $index = $this->counter % $count;
        $this->counter++;

        return array_merge(array_slice($nodes, $index), array_slice($nodes, 0, $index));
    }
}
