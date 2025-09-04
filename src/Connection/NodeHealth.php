<?php

declare(strict_types=1);

namespace Cassandra\Connection;

final class NodeHealth {
    private const FAILURE_COUNT_TO_DELAY = [
        0 => 0.5,
        1 => 0.5,
        2 => 1,
        3 => 2,
        4 => 4,
        5 => 8,
        6 => 16,
        7 => 32,
    ];

    /**
     * @var array<string,array{
     *   failures:int,
     *   cooldown_until:float
     * }>
     */
    private array $statusByKey = [];

    public function isAvailable(NodeConfig $config): bool {
        $key = $this->getKey($config);
        $status = $this->statusByKey[$key] ?? null;
        if ($status === null) {
            return true;
        }

        return $status['cooldown_until'] <= microtime(true);
    }

    /**
     * @param array<NodeConfig> $nodes
     * @return array{
     *  available: array<NodeConfig>,
     *  unavailable: array<NodeConfig>
     * }
     */
    public function partitionByAvailability(array $nodes): array {
        $available = [];
        $unavailable = [];
        foreach ($nodes as $node) {
            if ($this->isAvailable($node)) {
                $available[] = $node;
            } else {
                $unavailable[] = $node;
            }
        }

        return [
            'available' => $available,
            'unavailable' => $unavailable,
        ];
    }

    public function recordFailure(NodeConfig $config): void {
        $key = $this->getKey($config);
        $prevFailures = $this->statusByKey[$key]['failures'] ?? 0;
        $failures = $prevFailures + 1;
        $backoffSeconds = $this->computeBackoffSeconds($failures);
        $this->statusByKey[$key] = [
            'failures' => $failures,
            'cooldown_until' => microtime(true) + $backoffSeconds,
        ];
    }

    public function recordSuccess(NodeConfig $config): void {
        $key = $this->getKey($config);
        unset($this->statusByKey[$key]);
    }

    private function computeBackoffSeconds(int $failures): float {

        if (!isset(self::FAILURE_COUNT_TO_DELAY[$failures])) {
            return 32;
        }

        return self::FAILURE_COUNT_TO_DELAY[$failures];
    }

    private function getKey(NodeConfig $config): string {
        return $config->host . ':' . $config->port;
    }
}
