<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\NodeException;

/**
 * Which node to talk to, and how well each of them has been behaving.
 *
 * The configured nodes are put in the order the selection strategy asks for,
 * and the ones that have failed recently are tried last rather than dropped —
 * a cluster where every node is in its cooldown must still be reachable.
 *
 * @internal this is not part of the public API and may change at any time
 */
final class NodeConnector {
    private NodeHealth $health;

    /**
     * @var array<NodeConfig> $nodes
     */
    private array $nodes;

    private NodeSelector $selector;

    /**
     * @param array<NodeConfig> $nodes
     */
    public function __construct(array $nodes, NodeSelector $selector) {
        $this->nodes = $nodes;
        $this->selector = $selector;
        $this->health = new NodeHealth();
    }

    /**
     * Open a connection to the first node that accepts one.
     *
     * @throws \Cassandra\Exception\ConnectionException
     */
    public function open(): IoNode {

        $ordered = $this->selector->order($this->nodes);
        $parts = $this->health->partitionByAvailability($ordered);
        $candidates = array_merge($parts['available'], $parts['unavailable']);

        $socketException = null;

        foreach ($candidates as $config) {

            $className = $config->getNodeClass();

            try {
                $node = new $className($config);
                $node->connect();
            } catch (NodeException $e) {
                $socketException = $e;
                $this->health->recordFailure($config);

                continue;
            }

            $this->health->recordSuccess($config);

            return $node;
        }

        $nodeConfigs = array_map(fn (NodeConfig $config) => [
            'host' => $config->host,
            'port' => $config->port,
            'class' => $config->getNodeClass(),
        ], $this->nodes);

        throw new ConnectionException(
            'Unable to connect to any Cassandra node',
            ExceptionCode::CONNECTION_UNABLE_TO_CONNECT_ANY_NODE->value,
            [
                'attempted_nodes' => $nodeConfigs,
                'node_count' => count($this->nodes),
            ],
            $socketException ?? null
        );
    }

    public function recordFailure(NodeConfig $config): void {

        $this->health->recordFailure($config);
    }

    public function recordSuccess(NodeConfig $config): void {

        $this->health->recordSuccess($config);
    }
}
