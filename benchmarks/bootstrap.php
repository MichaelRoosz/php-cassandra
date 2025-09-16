<?php

declare(strict_types=1);

use Cassandra\Connection;
use Cassandra\Consistency;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Connection\StreamNodeConfig;

require __DIR__ . '/../vendor/autoload.php';

// Environment defaults aligned with integration tests
if (!getenv('APP_CASSANDRA_HOST')) {
    putenv('APP_CASSANDRA_HOST=127.0.0.1');
}
if (!getenv('APP_CASSANDRA_PORT')) {
    putenv('APP_CASSANDRA_PORT=9142');
}

/**
 * Provide a lazily created shared Connection for benchmarks.
 */
final class BenchEnv {
    public const DEFAULT_KEYSPACE = 'phpbenchks';
    private static ?Connection $connection = null;

    public static function connection(): Connection {
        if (self::$connection instanceof Connection) {
            return self::$connection;
        }

        $mode = getenv('APP_CASSANDRA_CONNECTION_MODE') ?: 'socket';
        $host = getenv('APP_CASSANDRA_HOST') ?: '127.0.0.1';
        $port = (int) (getenv('APP_CASSANDRA_PORT') ?: '9042');

        $nodes = $mode === 'stream'
            ? [new StreamNodeConfig(host: $host, port: $port, username: '', password: '')]
            : [new SocketNodeConfig(host: $host, port: $port, username: '', password: '')];

        // First, ensure keyspace exists using a system connection
        $systemConn = new Connection($nodes, 'system');
        $systemConn->setConsistency(Consistency::ONE);
        $systemConn->connect();
        self::ensureKeyspace($systemConn);
        $systemConn->disconnect();

        // Then connect to the benchmark keyspace and ensure tables
        $conn = new Connection($nodes, self::DEFAULT_KEYSPACE);
        $conn->setConsistency(Consistency::ONE);
        $conn->connect();
        self::ensureTables($conn);

        return self::$connection = $conn;
    }

    public static function reset(): void {
        if (self::$connection) {
            self::$connection->disconnect();
            self::$connection = null;
        }
    }

    private static function ensureKeyspace(Connection $conn): void {
        $keyspace = self::DEFAULT_KEYSPACE;
        $conn->query("CREATE KEYSPACE IF NOT EXISTS {$keyspace} WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}");
    }

    private static function ensureTables(Connection $conn): void {
        $conn->query('CREATE TABLE IF NOT EXISTS kv (id int PRIMARY KEY, v text)');
        $conn->query('CREATE TABLE IF NOT EXISTS big_kv (filename varchar, ukey varchar, value map<varchar, varchar>, PRIMARY KEY (filename, ukey))');
    }
}
