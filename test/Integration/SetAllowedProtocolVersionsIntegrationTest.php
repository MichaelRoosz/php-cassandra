<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Connection\ConnectionOptions;
use Cassandra\Protocol\ProtocolVersion;

final class SetAllowedProtocolVersionsIntegrationTest extends AbstractIntegrationTestCase {
    protected function setUp(): void {
        $this->keyspace = self::$defaultKeyspace;
    }

    public function testSetAllowedProtocolVersionsToV3(): void {

        $this->connection = $this->newConnection(
            self::$defaultKeyspace, 
            connect: false,
            options: new ConnectionOptions(
                allowedProtocolVersions: [ProtocolVersion::V3],
            ) 
        );

        $this->connection->registerWarningsListener($this);
        $this->connection->connect();

        $this->assertEquals($this->connection->getProtocolVersion(), ProtocolVersion::V3);
    }

    public function testSetAllowedProtocolVersionsToV3OrV4(): void {

        $this->connection = $this->newConnection(
            self::$defaultKeyspace, 
            connect: false,
            options: new ConnectionOptions(
                allowedProtocolVersions: [ProtocolVersion::V3, ProtocolVersion::V4],
            ) 
        );

        $this->connection->registerWarningsListener($this);
        $this->connection->connect();

        $this->assertEquals($this->connection->getProtocolVersion(), ProtocolVersion::V4);
    }
}
