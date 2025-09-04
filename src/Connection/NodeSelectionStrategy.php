<?php

declare(strict_types=1);

namespace Cassandra\Connection;

enum NodeSelectionStrategy {
    case Random;
    case RoundRobin;

    public function createSelector(): NodeSelector {
        return match ($this) {
            self::Random => new RandomSelector(),
            self::RoundRobin => new RoundRobinSelector(),
        };
    }
}
