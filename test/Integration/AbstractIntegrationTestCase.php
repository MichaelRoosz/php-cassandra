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

    protected static function getPort(): int {
        $port = getenv('APP_CASSANDRA_PORT') ?: '9042';

        return (int) $port;
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

    protected static function isScyllaDb(): bool {
        return getenv('APP_CASSANDRA_DB_TYPE') === 'scylladb';
    }

    protected static function isVectorDataTypeSupported(): bool {
        if (self::isScyllaDb()) {
            return true;
        }

        $cassandraVersion = getenv('CASSANDRA_VERSION');
        if ($cassandraVersion && version_compare($cassandraVersion, '5.0', '<')) {
            return false;
        }

        return true;
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
                username: '',
                password: ''
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
                username: '',
                password: ''
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
        $connection->query(
            "CREATE KEYSPACE {$keyspace} WITH REPLICATION = " .
            "{'class': 'SimpleStrategy', 'replication_factor': 1}"
        );
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
