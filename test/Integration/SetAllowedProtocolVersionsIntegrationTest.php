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
                initialProtocolVersion: ProtocolVersion::V3,
            ),
            forceInitialProtocolVersion: true
        );

        $this->connection->registerWarningsListener($this);
        $this->connection->connect();

        $this->assertEquals(ProtocolVersion::V3, $this->connection->getProtocolVersion());
    }

    public function testSetAllowedProtocolVersionsToV3OrV4(): void {

        if (self::isProtocolVersionSupported(ProtocolVersion::V4) === false) {
            $this->markTestSkipped('Protocol V4 is not supported by the server.');
        }

        if (self::isScyllaDb()) {
            $options = new ConnectionOptions(
                allowedProtocolVersions: [ProtocolVersion::V3, ProtocolVersion::V4],
                initialProtocolVersion: ProtocolVersion::V4,
            );
        } else {
            // Protocol negiotiation is supported since Cassandra 4.x,
            // and should pick V4 when both V3 and V4 are allowed.
            // If the server is older than 4.x, the protocol version is fixed to the inital version.
            $initialVersion = self::cassandraVersionIsAtLeast('4.0') ? ProtocolVersion::V3 : ProtocolVersion::V4;
            $options = new ConnectionOptions(
                allowedProtocolVersions: [ProtocolVersion::V3, ProtocolVersion::V4],
                initialProtocolVersion: $initialVersion,
            );
        }

        $this->connection = $this->newConnection(
            self::$defaultKeyspace,
            connect: false,
            options: $options,
            forceInitialProtocolVersion: true
        );

        $this->connection->registerWarningsListener($this);
        $this->connection->connect();

        $this->assertEquals(ProtocolVersion::V4, $this->connection->getProtocolVersion());
    }

    public function testSetAllowedProtocolVersionsToV4(): void {

        if (self::isProtocolVersionSupported(ProtocolVersion::V4) === false) {
            $this->markTestSkipped('Protocol V4 is not supported by the server.');
        }

        $this->connection = $this->newConnection(
            self::$defaultKeyspace,
            connect: false,
            options: new ConnectionOptions(
                allowedProtocolVersions: [ProtocolVersion::V4],
                initialProtocolVersion: ProtocolVersion::V4,
            ),
            forceInitialProtocolVersion: true
        );

        $this->connection->registerWarningsListener($this);
        $this->connection->connect();

        $this->assertEquals(ProtocolVersion::V4, $this->connection->getProtocolVersion());
    }

    public function testSetAllowedProtocolVersionsToV5(): void {

        if (self::isProtocolVersionSupported(ProtocolVersion::V5) === false) {
            $this->markTestSkipped('Protocol V5 is not supported by the server.');
        }

        $this->connection = $this->newConnection(
            self::$defaultKeyspace,
            connect: false,
            options: new ConnectionOptions(
                allowedProtocolVersions: [ProtocolVersion::V5],
                initialProtocolVersion: ProtocolVersion::V5,
            ),
            forceInitialProtocolVersion: true
        );

        $this->connection->registerWarningsListener($this);
        $this->connection->connect();

        $this->assertEquals(ProtocolVersion::V5, $this->connection->getProtocolVersion());
    }
}
