<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Protocol\ProtocolVersion;

final class SetAllowedProtocolVersionsIntegrationTest extends AbstractIntegrationTestCase {
    protected function setUp(): void {

        $this->connection = $this->newConnection(self::$defaultKeyspace, connect: false);
        $this->connection->registerWarningsListener($this);
        $this->keyspace = self::$defaultKeyspace;
    }

    public function testSetAllowedProtocolVersionsWhenConnectedThrows(): void {

        $this->connection->connect();

        $this->expectException(ConnectionException::class);
        $this->expectExceptionCode(ExceptionCode::CONNECTION_SET_ALLOWED_PROTOCOL_VERSIONS_WHEN_ALREADY_CONNECTED->value);

        $this->connection->setAllowedProtocolVersions([
            ProtocolVersion::V3,
        ]);
    }

    public function testSetAllowedProtocolVersionsWhenNotConnected(): void {

        $allowedVersions = [
            ProtocolVersion::V3,
        ];
        $this->connection->setAllowedProtocolVersions($allowedVersions);

        $this->assertSame($allowedVersions, $this->connection->getAllowedProtocolVersions());

        $this->connection->connect();
        $this->assertContains($this->connection->getProtocolVersion(), $allowedVersions);
    }
}
