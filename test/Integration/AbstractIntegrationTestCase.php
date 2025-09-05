<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Connection;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Consistency;
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

    public function onWarnings(Response $response, array $warnings): void {

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

    protected static function getHost(): string {
        return getenv('APP_CASSANDRA_HOST') ?: '127.0.0.1';
    }

    protected static function getPort(): int {
        $port = getenv('APP_CASSANDRA_PORT') ?: '9042';

        return (int) $port;
    }

    protected static function newConnection(string $keyspace): Connection {
        $nodes = [
            new SocketNodeConfig(
                host: self::getHost(),
                port: self::getPort(),
                username: '',
                password: ''
            ),
        ];

        $conn = new Connection($nodes, $keyspace);
        $conn->setConsistency(Consistency::ONE);
        $conn->connect();

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
