<?php

declare(strict_types=1);

namespace Cassandra\Connection;

final class RandomSelector implements NodeSelector {
    #[\Override]
    public function order(array $nodes): array {
        if (count($nodes) > 1) {
            $copy = $nodes;
            shuffle($copy);

            return $copy;
        }

        return $nodes;
    }
}
