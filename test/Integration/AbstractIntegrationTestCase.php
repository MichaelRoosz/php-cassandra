<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Connection;
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\Consistency;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Request;
use Cassandra\Response\Response;
use Cassandra\WarningsListener;
use PHPUnit\Framework\TestCase;

abstract class AbstractIntegrationTestCase extends TestCase implements WarningsListener {
    protected Connection $connection;

    protected static string $defaultKeyspace = 'phpunit';
    protected string $keyspace;

    public static function setUpBeforeClass(): void {

        self::$defaultKeyspace = self::calculateKeyspaceName();
        self::setupKeyspace();
        static::setupTable();
    }

    public static function tearDownAfterClass(): void {

        self::teardownKeyspace();
    }

    protected function setUp(): void {

        $this->connection = $this->newConnection(self::$defaultKeyspace);
        $this->connection->registerWarningsListener($this);
        $this->keyspace = self::$defaultKeyspace;
    }

    protected function tearDown(): void {

        $this->connection->disconnect();
    }

    public function integerHasAtLeast64Bits(): bool {
        return PHP_INT_SIZE >= 8;
    }

    public function onWarnings(array $warnings, Request $request, Response $response): void {

        $this->fail('Received warnings: ' . implode(', ', $warnings));
    }

    protected static function calculateKeyspaceName(): string {

        $keyspace = static::class;
        $keyspace = str_replace('Cassandra\\Test\\Integration\\', '', $keyspace);
        $keyspace = str_replace('\\', '', $keyspace);

        if (strlen($keyspace) > 48) {
            return substr($keyspace, 0, 48);
        }

        $keyspace = strtolower($keyspace);

        return $keyspace;
    }

    protected static function cassandraVersionIsAtLeast(string $version): bool {

        if (self::isScyllaDb()) {
            return false;
        }

        $cassandraVersion = getenv('CASSANDRA_VERSION');
        if ($cassandraVersion && version_compare($cassandraVersion, $version, '<')) {
            return false;
        }

        return true;
    }

    protected static function cqlshSupportsVectorsWithDynamicLengthDataType(): bool {
        if (self::isScyllaDb()) {
            // note: scylladb's cqlsh does not support vectors currently
            return false;
        }

        // note: cqlsh does not support vectors of dynamic length data types currently
        return false;
    }

    protected static function cqlshSupportsVectorsWithFixedLengthDataType(): bool {
        if (self::isScyllaDb()) {
            // note: scylladb's cqlsh does not support vectors currently
            return false;
        }

        return true;
    }

    protected static function getHost(): string {
        return getenv('APP_CASSANDRA_HOST') ?: '127.0.0.1';
    }

    protected static function getPassword(): string {
        return getenv('APP_CASSANDRA_PASSWORD') ?: '';
    }

    protected static function getPort(): int {
        $port = getenv('APP_CASSANDRA_PORT') ?: '9042';

        return (int) $port;
    }

    protected static function getUsername(): string {
        return getenv('APP_CASSANDRA_USERNAME') ?: '';
    }

    /**
     * Arithmetic expressions in the selection clause (e.g. "n + 5") were added
     * in Cassandra 4.0 (CASSANDRA-11935) and are not supported by ScyllaDB.
     */
    protected static function isArithmeticInSelectClauseSupported(): bool {

        if (self::isScyllaDb()) {
            return false;
        }

        return self::cassandraVersionIsAtLeast('4.0');
    }

    /**
     * True when the test cluster runs with authentication and authorization
     * enabled (see docker-compose.auth.yml).
     */
    protected static function isAuthEnabled(): bool {
        return getenv('APP_CASSANDRA_AUTH_ENABLED') === '1';
    }

    /**
     * Bind markers as function-call arguments in the selection clause were added
     * in Cassandra 3.6 (CASSANDRA-10783). ScyllaDB rejects them with
     * "Bind variables cannot be used for keyspace names".
     */
    protected static function isBindMarkerInFunctionArgumentSupported(): bool {

        if (self::isScyllaDb()) {
            return false;
        }

        return self::cassandraVersionIsAtLeast('3.6');
    }

    /**
     * The CAST() function in the selection clause was added in Cassandra 3.2
     * (CASSANDRA-10310). ScyllaDB supports it.
     */
    protected static function isCastFunctionSupported(): bool {

        if (self::isScyllaDb()) {
            return true;
        }

        return self::cassandraVersionIsAtLeast('3.2');
    }

    /**
     * Sending a request with the CUSTOM_PAYLOAD frame flag set. Stock Cassandra
     * accepts (and ignores) the payload. ScyllaDB never parsed the request-side
     * custom payload [bytes map]: it left the bytes in the frame and mis-parsed
     * the body, failing with a "truncated frame" protocol error (verified
     * against ScyllaDB 6.2 over protocol v4). This is SCYLLADB-745, fixed on
     * ScyllaDB master on 2026-05-21; once a release that contains the fix is
     * tested, add a `version_compare(getenv('SCYLLADB_VERSION'), ...)` gate here
     * instead of skipping every ScyllaDB version.
     */
    protected static function isCustomPayloadSupported(): bool {

        return !self::isScyllaDb();
    }

    /**
     * DESCRIBE over CQL was added in Cassandra 4.0 (CASSANDRA-14825). ScyllaDB
     * returns a different result shape and is excluded.
     */
    protected static function isDescribeOverCqlSupported(): bool {

        if (self::isScyllaDb()) {
            return false;
        }

        return self::cassandraVersionIsAtLeast('4.0');
    }

    /**
     * Zero-length elements inside a vector (e.g. an empty string in a
     * vector<text, n>). Cassandra stores them as an element with a
     * zero-length VInt prefix; ScyllaDB treats a zero-length element as
     * null and rejects the write with
     * "[INVALID 8704] null/unset is not supported inside vectors".
     */
    protected static function isEmptyValueInsideVectorSupported(): bool {

        return !self::isScyllaDb();
    }

    /**
     * GROUP BY was added in Cassandra 3.10 (CASSANDRA-10707). ScyllaDB supports
     * it. (PER PARTITION LIMIT, added in Cassandra 3.6, is subsumed by this.)
     */
    protected static function isGroupBySupported(): bool {

        if (self::isScyllaDb()) {
            return true;
        }

        return self::cassandraVersionIsAtLeast('3.10');
    }

    /**
     * The "INSERT ... JSON ? DEFAULT NULL|UNSET" clause was added in Cassandra
     * 3.10 (CASSANDRA-11424). ScyllaDB supports it.
     */
    protected static function isInsertJsonDefaultClauseSupported(): bool {

        if (self::isScyllaDb()) {
            return true;
        }

        return self::cassandraVersionIsAtLeast('3.10');
    }

    protected static function isProtocolVersionSupported(ProtocolVersion $version): bool {

        if (self::isScyllaDb()) {
            return in_array($version, [
                ProtocolVersion::V3,
                ProtocolVersion::V4,
            ], true);
        }

        /*
        * v3: supported in Cassandra 2.1-->3.x+
        * v4: supported in Cassandra 2.2-->3.x+
        * v5: in beta from 3.x+. Finalised in 4.0-beta5
        */
        $neededVersion = match ($version) {
            ProtocolVersion::V3 => '2.1',
            ProtocolVersion::V4 => '2.2',
            ProtocolVersion::V5 => '4.0',
        };

        $cassandraVersion = getenv('CASSANDRA_VERSION');
        if ($cassandraVersion && version_compare($cassandraVersion, $neededVersion, '<')) {
            return false;
        }

        return true;
    }

    /**
     * SASI is a Cassandra-only custom index implementation (available since
     * Cassandra 3.4, CASSANDRA-10661). ScyllaDB does not support it.
     */
    protected static function isSasiIndexSupported(): bool {

        if (self::isScyllaDb()) {
            return false;
        }

        return self::cassandraVersionIsAtLeast('3.4');
    }

    protected static function isScyllaDb(): bool {
        return getenv('APP_CASSANDRA_DB_TYPE') === 'scylladb';
    }

    protected static function isVectorDataTypeSupported(): bool {
        if (self::isScyllaDb()) {
            $scyllaDbVersion = getenv('SCYLLADB_VERSION');
            if ($scyllaDbVersion && version_compare($scyllaDbVersion, '2025.2', '<')) {
                return false;
            }

            return true;
        }

        $cassandraVersion = getenv('CASSANDRA_VERSION');
        if ($cassandraVersion && version_compare($cassandraVersion, '5.0', '<')) {
            return false;
        }

        return true;
    }

    /**
     * Virtual tables were introduced in Cassandra 4.0, but the
     * system_views.settings rows queried by the tests only exist from 4.1.
     * ScyllaDB exposes a different set and is excluded.
     */
    protected static function isVirtualTablesSupported(): bool {

        if (self::isScyllaDb()) {
            return false;
        }

        return self::cassandraVersionIsAtLeast('4.1');
    }

    protected static function newConnection(
        string $keyspace,
        bool $connect = true,
        ConnectionOptions $options = new ConnectionOptions(),
        bool $forceInitialProtocolVersion = false
    ): Connection {

        if (self::isScyllaDb()) {
            $options = new ConnectionOptions(
                enableCompression: $options->enableCompression,
                throwOnOverload: $options->throwOnOverload,
                nodeSelectionStrategy: $options->nodeSelectionStrategy,
                preparedResultCacheSize: $options->preparedResultCacheSize,
                allowedProtocolVersions: $options->allowedProtocolVersions,
                initialProtocolVersion: $forceInitialProtocolVersion ? $options->initialProtocolVersion : ProtocolVersion::V4,
            );
        }

        $mode = getenv('APP_CASSANDRA_CONNECTION_MODE') ?: 'socket';

        return match ($mode) {
            'socket' => self::newSocketConnection($keyspace, $connect, $options),
            'stream' => self::newStreamConnection($keyspace, $connect, $options),
            default => self::newSocketConnection($keyspace, $connect, $options),
        };
    }

    protected static function newSocketConnection(
        string $keyspace,
        bool $connect = true,
        ConnectionOptions $options = new ConnectionOptions()
    ): Connection {

        $nodes = [
            new SocketNodeConfig(
                host: self::getHost(),
                port: self::getPort(),
                username: self::getUsername(),
                password: self::getPassword()
            ),
        ];

        $conn = new Connection($nodes, $keyspace, $options);
        $conn->setConsistency(Consistency::ONE);

        if ($connect) {
            $conn->connect();
        }

        return $conn;
    }

    protected static function newStreamConnection(
        string $keyspace,
        bool $connect = true,
        ConnectionOptions $options = new ConnectionOptions()
    ): Connection {

        $nodes = [
            new StreamNodeConfig(
                host: self::getHost(),
                port: self::getPort(),
                username: self::getUsername(),
                password: self::getPassword()
            ),
        ];

        $conn = new Connection($nodes, $keyspace, $options);
        $conn->setConsistency(Consistency::ONE);

        if ($connect) {
            $conn->connect();
        }

        return $conn;
    }

    protected static function setupKeyspace(): void {

        $keyspace = self::$defaultKeyspace;
        $connection = self::newConnection('system');
        $connection->query("DROP KEYSPACE IF EXISTS {$keyspace}");

        if (self::isScyllaDb()) {
            $connection->query(
                "CREATE KEYSPACE {$keyspace} WITH REPLICATION = " .
                "{'class': 'NetworkTopologyStrategy', 'dc1': 1} AND TABLETS = {'enabled': false};"
            );
        } else {
            $connection->query(
                "CREATE KEYSPACE {$keyspace} WITH REPLICATION = " .
                "{'class': 'NetworkTopologyStrategy', 'dc1': 1}"
            );
        }
        $connection->disconnect();
    }

    protected static function setupTable(): void {
        // empty method to be overridden by subclasses
    }

    protected static function teardownKeyspace(): void {

        $keyspace = self::$defaultKeyspace;
        $connection = self::newConnection('system');
        $connection->query("DROP KEYSPACE IF EXISTS {$keyspace}");
        $connection->disconnect();
    }
}
