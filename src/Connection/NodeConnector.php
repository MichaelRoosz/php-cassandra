<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Exception\CassandraException;
use Cassandra\Exception\CompressionException;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\NodeException;
use Cassandra\Exception\RequestException;
use Cassandra\Exception\RequestTimeoutException;
use Cassandra\Exception\ResponseException;
use Cassandra\Exception\ServerException;
use Cassandra\Exception\ValueException;
use Cassandra\Exception\ValueFactoryException;
use Throwable;

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
     * Open a connection to the first node that accepts one and passes the
     * caller's optional post-connect validation (the Cassandra handshake).
     *
     * @param ?callable(IoNode): void $afterConnect
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ServerException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    public function open(?callable $afterConnect = null): IoNode {

        $ordered = $this->orderNodes();
        foreach ($ordered as $index => $config) {
            if (!$config instanceof NodeConfig) {
                throw new NodeException(
                    'Node selector returned an invalid node configuration',
                    ExceptionCode::NODE_IMPLEMENTATION_FAILED->value,
                    [
                        'selector_class' => get_class($this->selector),
                        'index' => $index,
                        'actual_type' => get_debug_type($config),
                    ]
                );
            }
        }

        $parts = $this->health->partitionByAvailability($ordered);
        $candidates = array_merge($parts['available'], $parts['unavailable']);

        $lastNodeException = null;
        $lastHandshakeException = null;

        foreach ($candidates as $config) {

            $className = $config->getNodeClass();

            if (!is_a($className, IoNode::class, true)) {
                $lastNodeException = new NodeException(
                    'Invalid node implementation; configured class must implement IoNode',
                    ExceptionCode::NODE_IMPLEMENTATION_FAILED->value,
                    [
                        'configured_class' => $className,
                        'required_interface' => IoNode::class,
                    ]
                );
                $this->health->recordFailure($config);

                continue;
            }

            try {
                $node = new $className($config);
                $node->connect();
            } catch (NodeException $e) {
                $lastNodeException = $e;
                $this->health->recordFailure($config);

                continue;
            } catch (Throwable $e) {
                $lastNodeException = new NodeException(
                    'Node implementation failed',
                    ExceptionCode::NODE_IMPLEMENTATION_FAILED->value,
                    [
                        'configured_class' => $className,
                        'host' => $config->host,
                        'port' => $config->port,
                    ],
                    $e
                );
                $this->health->recordFailure($config);

                continue;
            }

            if ($afterConnect !== null) {
                try {
                    $afterConnect($node);
                } catch (CassandraException $e) {
                    $lastHandshakeException = $e;
                    $this->health->recordFailure($config);
                    $node->close();

                    continue;
                } catch (Throwable $e) {
                    $lastHandshakeException = new ConnectionException(
                        'Connection handshake failed unexpectedly',
                        ExceptionCode::CONNECTION_HANDSHAKE_FAILED->value,
                        [
                            'host' => $config->host,
                            'port' => $config->port,
                        ],
                        $e,
                    );
                    $this->health->recordFailure($config);

                    $node->close();

                    continue;
                }
            }

            $this->health->recordSuccess($config);

            return $node;
        }

        if ($lastHandshakeException !== null) {
            $this->rethrowHandshakeException($lastHandshakeException);
        }

        throw $this->unableToConnectException($lastNodeException);
    }

    public function recordFailure(NodeConfig $config): void {

        $this->health->recordFailure($config);
    }

    public function recordSuccess(NodeConfig $config): void {

        $this->health->recordSuccess($config);
    }

    /**
     * @return array<NodeConfig>
     */
    private function orderNodes(): array {
        return $this->selector->order($this->nodes);
    }

    /**
     * Preserve the specific project exception that explains why the last
     * Cassandra handshake failed after every configured node was attempted.
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ServerException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    private function rethrowHandshakeException(CassandraException $exception): never {
        if ($exception instanceof CompressionException) {
            throw $exception;
        }
        if ($exception instanceof ConnectionException) {
            throw $exception;
        }
        if ($exception instanceof NodeException) {
            throw $exception;
        }
        if ($exception instanceof RequestException) {
            throw $exception;
        }
        if ($exception instanceof RequestTimeoutException) {
            throw $exception;
        }
        if ($exception instanceof ResponseException) {
            throw $exception;
        }
        if ($exception instanceof ServerException) {
            throw $exception;
        }
        if ($exception instanceof ValueException) {
            throw $exception;
        }
        if ($exception instanceof ValueFactoryException) {
            throw $exception;
        }

        throw new ConnectionException(
            'Connection handshake failed with an unexpected project exception',
            ExceptionCode::CONNECTION_HANDSHAKE_FAILED->value,
            ['exception_class' => get_class($exception)],
            $exception,
        );
    }

    private function unableToConnectException(?Throwable $previous): ConnectionException {
        $nodeConfigs = array_map(fn (NodeConfig $config) => [
            'host' => $config->host,
            'port' => $config->port,
            'config_class' => get_class($config),
        ], $this->nodes);

        return new ConnectionException(
            'Unable to connect to any Cassandra node',
            ExceptionCode::CONNECTION_UNABLE_TO_CONNECT_ANY_NODE->value,
            [
                'attempted_nodes' => $nodeConfigs,
                'node_count' => count($this->nodes),
            ],
            $previous
        );
    }
}
