<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Consistency;

final class ConnectionTest extends AbstractIntegrationTestCase {
    public function testAQueryOnAConnectionThatWasNeverConnectedUsesItsKeyspace(): void {
        // The keyspace has to survive the connection being opened by the query
        // itself rather than beforehand. How it is applied depends on the
        // negotiated protocol version — from v5 it travels with the request,
        // before that a USE settles it at handshake time — so a connection that
        // has not yet handshaked knows neither which of the two to do nor what
        // the version is going to be. Getting that wrong is silent: the query
        // simply runs against no keyspace and the coordinator refuses it.
        $conn = self::newConnection(self::$defaultKeyspace, connect: false);

        try {
            $this->assertFalse($conn->isConnected());

            // The very first statement on the connection, and unqualified, so
            // it can only land anywhere at all if the keyspace was applied.
            $conn->query('CREATE TABLE IF NOT EXISTS lazy_connect_probe(id int PRIMARY KEY)');

            $rows = $conn->query('SELECT id FROM lazy_connect_probe')->asRowsResult();

            $this->assertTrue($conn->isConnected());
            $this->assertSame(0, $rows->getRowCount());
        } finally {
            $conn->disconnect();
        }
    }

    public function testConnectAndProtocolNegotiation(): void {

        $conn = $this->connection;
        $conn->connect();
        $this->assertTrue($conn->isConnected());

        $version = $conn->getProtocolVersion();
        $this->assertTrue($version->supports(ProtocolVersion::V3));
        $this->assertGreaterThanOrEqual(ProtocolVersion::V3->value, $version->value);

        if ($version->value >= ProtocolVersion::V5->value) {
            $this->assertTrue($conn->supportsKeyspaceRequestOption());
            $this->assertTrue($conn->supportsNowInSecondsRequestOption());
        }
    }

    public function testSetConsistencyAffectsDefault(): void {

        $conn = $this->connection;
        $conn->setConsistency(Consistency::ONE);
        $rows = $conn->query('SELECT key FROM system.local')->asRowsResult();
        $this->assertSame(1, $rows->getRowCount());
        $row = $rows->fetch();
        $this->assertSame(['key' => 'local'], $row);
    }
}
