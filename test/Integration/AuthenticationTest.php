<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Connection;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Consistency;
use Cassandra\Exception\ServerException\UnauthorizedException;
use Cassandra\Response\Result\RowsResult;

/**
 * Integration tests for authentication, roles and permissions.
 *
 * See https://docs.datastax.com/en/cql-oss/3.3/cql/cql_using/useSecureTable.html
 *
 * These require a cluster started with `PasswordAuthenticator` and
 * `CassandraAuthorizer` - see docker-compose.auth.yml and the
 * `test:integration_auth` composer scripts. Everything is skipped otherwise, so
 * the class is inert against the default unauthenticated test clusters.
 */
final class AuthenticationTest extends AbstractIntegrationTestCase {
    private const TEST_ROLE = 'phpunit_role';
    private const TEST_ROLE_PASSWORD = 'phpunit_pw';

    public static function setUpBeforeClass(): void {
        if (!self::isAuthEnabled()) {
            return;
        }

        parent::setUpBeforeClass();
    }

    public static function tearDownAfterClass(): void {
        if (!self::isAuthEnabled()) {
            return;
        }

        parent::tearDownAfterClass();
    }

    protected function setUp(): void {
        if (!self::isAuthEnabled()) {
            $this->markTestSkipped(
                'Requires a cluster with authentication enabled - run "composer test:integration_auth"'
            );
        }

        parent::setUp();
    }

    protected function tearDown(): void {
        if (!self::isAuthEnabled()) {
            return;
        }

        parent::tearDown();
    }

    public function testAuthenticatedConnectionSucceeds(): void {
        $row = $this->connection->query('SELECT release_version FROM system.local')
            ->asRowsResult()->fetch();

        $this->assertIsArray($row);
        $this->assertIsString($row['release_version']);
    }

    public function testCreateAlterAndDropRole(): void {
        $this->createTestRole();

        $this->assertContains(self::TEST_ROLE, $this->listRoleNames(), 'The new role should be listed');

        $this->assertTrue($this->getRoleLoginFlag(), 'A new role with LOGIN = true can log in');

        $this->connection->query('ALTER ROLE ' . self::TEST_ROLE . ' WITH LOGIN = false');

        $this->assertFalse(
            $this->getRoleLoginFlag(),
            'ALTER ROLE should have revoked the LOGIN privilege'
        );

        $this->connection->query('DROP ROLE IF EXISTS ' . self::TEST_ROLE);

        $this->assertNotContains(self::TEST_ROLE, $this->listRoleNames(), 'The role should be gone');
    }

    public function testGrantAndRevokePermissions(): void {
        $this->createTestRole();

        $this->connection->query(
            'GRANT SELECT ON KEYSPACE ' . $this->keyspace . ' TO ' . self::TEST_ROLE
        );

        $granted = $this->listPermissions();
        $this->assertContains('SELECT', $granted, 'The granted permission should be listed');

        $this->connection->query(
            'REVOKE SELECT ON KEYSPACE ' . $this->keyspace . ' FROM ' . self::TEST_ROLE
        );

        $this->assertNotContains('SELECT', $this->listPermissions(), 'The permission should be revoked');

        $this->connection->query('DROP ROLE IF EXISTS ' . self::TEST_ROLE);
    }

    public function testUnauthorizedAccessIsRejected(): void {
        $this->createTestRole();
        $this->connection->query('CREATE TABLE IF NOT EXISTS auth_secrets (id int PRIMARY KEY, v varchar)');

        // The new role has no permissions at all, so reading must be refused.
        $restricted = $this->newRoleConnection();

        try {
            $restricted->query("SELECT * FROM {$this->keyspace}.auth_secrets");
            $this->fail('Reading without a GRANT should have been rejected');
        } catch (UnauthorizedException $e) {
            $this->assertStringContainsStringIgnoringCase('unauthorized', $e->getMessage());
        } finally {
            $restricted->disconnect();
        }

        // After a GRANT the same role can read.
        $this->connection->query(
            'GRANT SELECT ON KEYSPACE ' . $this->keyspace . ' TO ' . self::TEST_ROLE
        );

        $allowed = $this->newRoleConnection();

        try {
            $rows = $allowed->query("SELECT * FROM {$this->keyspace}.auth_secrets")->asRowsResult();
            $this->assertSame(0, $rows->getRowCount());
        } finally {
            $allowed->disconnect();
        }

        $this->connection->query(
            'REVOKE SELECT ON KEYSPACE ' . $this->keyspace . ' FROM ' . self::TEST_ROLE
        );
        $this->connection->query('DROP ROLE IF EXISTS ' . self::TEST_ROLE);
    }

    public function testWrongPasswordIsRejected(): void {
        $this->createTestRole();

        $conn = new Connection(
            [new SocketNodeConfig(
                host: self::getHost(),
                port: self::getPort(),
                username: self::TEST_ROLE,
                password: 'definitely-not-the-password',
            )],
            $this->keyspace
        );
        $conn->setConsistency(Consistency::ONE);

        try {
            $conn->connect();
            $this->fail('Connecting with a wrong password should have failed');
        } catch (\Cassandra\Exception\CassandraException $e) {
            $this->assertNotSame('', $e->getMessage());
        }

        $this->connection->query('DROP ROLE IF EXISTS ' . self::TEST_ROLE);
    }

    protected static function setupTable(): void {
        $conn = self::newConnection(self::$defaultKeyspace);
        $conn->query('CREATE TABLE IF NOT EXISTS auth_secrets (id int PRIMARY KEY, v varchar)');
        $conn->disconnect();
    }

    private function createTestRole(): void {
        // Drop first: the tests alter the role, so recreating it keeps each test
        // independent of the order they run in.
        $this->connection->query('DROP ROLE IF EXISTS ' . self::TEST_ROLE);

        // Role management statements do not accept bind markers, so the password
        // has to be inlined as a string literal.
        $this->connection->query(
            'CREATE ROLE ' . self::TEST_ROLE
            . " WITH PASSWORD = '" . self::TEST_ROLE_PASSWORD . "' AND LOGIN = true"
        );
    }

    private function getRoleLoginFlag(): ?bool {
        $roles = $this->connection->query('LIST ROLES')->asRowsResult()->fetchAll();

        foreach ($roles as $role) {
            if ($role['role'] === self::TEST_ROLE) {
                return is_bool($role['login']) ? $role['login'] : null;
            }
        }

        return null;
    }

    /**
     * @return list<string>
     */
    private function listPermissions(): array {
        $result = $this->connection->query('LIST ALL PERMISSIONS OF ' . self::TEST_ROLE);

        // With no permissions granted the server answers with a void result
        // rather than an empty rows result.
        if (!($result instanceof RowsResult)) {
            return [];
        }

        /** @var list<string> $permissions */
        $permissions = array_column($result->fetchAll(), 'permission');

        return $permissions;
    }

    /**
     * @return list<string>
     */
    private function listRoleNames(): array {
        $rows = $this->connection->query('LIST ROLES')->asRowsResult()->fetchAll();

        /** @var list<string> $names */
        $names = array_column($rows, 'role');

        return $names;
    }

    private function newRoleConnection(): Connection {
        $conn = new Connection(
            [new SocketNodeConfig(
                host: self::getHost(),
                port: self::getPort(),
                username: self::TEST_ROLE,
                password: self::TEST_ROLE_PASSWORD,
            )],
            $this->keyspace
        );
        $conn->setConsistency(Consistency::ONE);
        $conn->connect();

        return $conn;
    }
}
