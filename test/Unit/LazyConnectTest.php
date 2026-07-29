<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection;
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Exception\ConnectionException;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Options\BatchOptions;
use Cassandra\Request\Options\QueryOptions;

/**
 * That building a request opens the connection first.
 *
 * Whether a request carries the connection's keyspace depends on the negotiated
 * protocol version — from v5 it travels with the request, before that it was
 * settled with a USE at handshake time — and until the handshake has happened
 * there is only the *initial* version to go by. That guess is wrong in both
 * directions and neither way of being wrong is loud: guessed low against a v5
 * node the keyspace is dropped from the first request, which then runs against
 * whatever the coordinator's default is; guessed high against a v4 node an
 * option is attached that encoding the request refuses.
 *
 * So the connection is opened before the question is asked. Nothing here needs a
 * server to show that: a node that refuses the connection makes "did it try?"
 * observable, and a request built without connecting could not have.
 */
final class LazyConnectTest extends AbstractUnitTestCase {
    public function testBuildingABatchRequestOpensTheConnection(): void {
        $connection = $this->unreachableConnection('some_keyspace');

        $this->expectException(ConnectionException::class);

        $connection->createBatchRequest();
    }

    public function testCapabilityQueriesOpenTheConnection(): void {
        // Answering these from the initial protocol version would be reporting
        // a guess as a fact about the node.
        $connection = $this->unreachableConnection('some_keyspace');

        $this->expectException(ConnectionException::class);

        $connection->supportsKeyspaceRequestOption();
    }

    public function testNothingIsOpenedForAConnectionWithoutAKeyspace(): void {
        // With no keyspace of its own there is nothing for the connection to
        // fill in, so there is nothing to find out from the node either and a
        // request can be built without one.
        $connection = $this->unreachableConnection('');

        $batchRequest = $connection->createBatchRequest();

        $this->assertNull($batchRequest->getOptions()->keyspace);
        $this->assertFalse($connection->isConnected());
    }

    public function testNothingIsOpenedForAKeyspaceTheCallerAskedForThemselves(): void {
        // An explicit keyspace is the caller's to be right about; the
        // connection's own is only ever a default, and only the default needs
        // the negotiated version to be known.
        $connection = $this->unreachableConnection('some_keyspace');

        $batchRequest = $connection->createBatchRequest(
            options: new BatchOptions(keyspace: 'explicit'),
        );

        $this->assertSame('explicit', $batchRequest->getOptions()->keyspace);
        $this->assertFalse($connection->isConnected());
    }

    public function testQueryingOpensTheConnectionBeforeBuildingTheRequest(): void {
        $connection = $this->unreachableConnection('some_keyspace');

        $this->expectException(ConnectionException::class);

        $connection->query('SELECT * FROM t', options: new QueryOptions());
    }

    /**
     * A connection to a port nothing is listening on, so that connecting fails
     * at once and locally.
     *
     * @throws \Cassandra\Exception\ConnectionException
     */
    private function unreachableConnection(string $keyspace): Connection {

        return new Connection(
            [new SocketNodeConfig(host: '127.0.0.1', port: 1, connectTimeoutInSeconds: 1.0)],
            $keyspace,
            new ConnectionOptions(initialProtocolVersion: ProtocolVersion::V4),
        );
    }
}
