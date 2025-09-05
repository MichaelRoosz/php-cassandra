<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Consistency;

final class ConnectionTest extends AbstractIntegrationTestCase {
    public function testConnectAndProtocolNegotiation(): void {

        $conn = $this->connection;
        $conn->connect();
        $this->assertTrue($conn->isConnected());

        $version = $conn->getVersion();
        $this->assertGreaterThanOrEqual(3, $version);

        if ($version >= 5) {
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
