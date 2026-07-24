<?php

declare(strict_types=1);

use Cassandra\Connection;
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Consistency;
use Cassandra\Connection\IoNode;
use Cassandra\Connection\NodeConfig;
use Cassandra\Connection\NodeImplementation;
use Cassandra\Connection\Socket;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\Request\Request;
use Cassandra\Value\Blob;

require __DIR__ . '/../vendor/autoload.php';

/**
 * Node configuration that produces a {@see ThrottledSocket}, so benchmarks can
 * simulate a bandwidth-limited network link on top of a real Cassandra node.
 *
 * A `bytesPerSecond` of 0 means "unlimited" (full local speed).
 */
final class ThrottledSocketNodeConfig extends NodeConfig {
    public function __construct(
        string $host = 'localhost',
        int $port = 9042,
        string $username = '',
        string $password = '',
        public readonly int $bytesPerSecond = 0,
    ) {
        parent::__construct(host: $host, port: $port, username: $username, password: $password);
    }

    #[\Override]
    public function getNodeClass(): string {
        return ThrottledSocket::class;
    }
}

/**
 * A real socket transport that sleeps in proportion to the number of bytes it
 * sends and receives, simulating a fixed-bandwidth network link.
 *
 * This is what lets the compression benchmark answer the real question: on a
 * slow link, do the bytes saved by compressing frames outweigh the CPU time
 * spent compressing them in plain PHP?
 */
final class ThrottledSocket extends NodeImplementation implements IoNode {
    private readonly int $bytesPerSecond;

    private readonly NodeConfig $config;

    private readonly Socket $inner;

    public function __construct(NodeConfig $config) {
        $this->config = $config;
        $this->bytesPerSecond = $config instanceof ThrottledSocketNodeConfig ? $config->bytesPerSecond : 0;

        $this->inner = new Socket(new SocketNodeConfig(
            host: $config->host,
            port: $config->port,
            username: $config->username,
            password: $config->password,
        ));
    }

    #[\Override]
    public function close(): void {
        $this->inner->close();
    }

    #[\Override]
    public function connect(): void {
        $this->inner->connect();
    }

    #[\Override]
    public function getConfig(): NodeConfig {
        return $this->config;
    }

    #[\Override]
    public function readAvailableDataFromSource(int $expectedLength, int $upperBoundaryLength, bool $waitForData): string {
        $data = $this->inner->readAvailableDataFromSource($expectedLength, $upperBoundaryLength, $waitForData);
        $this->throttle(strlen($data));

        return $data;
    }

    #[\Override]
    public function write(string $data): void {
        $this->throttle(strlen($data));
        $this->inner->write($data);
    }

    #[\Override]
    public function writeRequest(Request $request): void {
        $this->write($request->__toString());
    }

    private function throttle(int $bytes): void {
        if ($this->bytesPerSecond > 0 && $bytes > 0) {
            $microseconds = (int) round($bytes / $this->bytesPerSecond * 1_000_000);
            if ($microseconds > 0) {
                usleep($microseconds);
            }
        }
    }
}

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
    public const COMPRESSION_KEYSPACE = 'phpbenchcompress';
    public const COMPRESSION_ROW_ID = 1;
    public const DEFAULT_KEYSPACE = 'phpbenchks';
    private static ?Connection $connection = null;

    /**
     * Open a fresh (uncached) connection to the compression-benchmark keyspace,
     * optionally with LZ4 compression enabled and/or a simulated bandwidth cap.
     *
     * @param int $bytesPerSecond 0 for full local speed, otherwise the
     *                            simulated network bandwidth in bytes/second.
     */
    public static function compressionConnection(bool $compress, int $bytesPerSecond = 0): Connection {
        $host = getenv('APP_CASSANDRA_HOST') ?: '127.0.0.1';
        $port = (int) (getenv('APP_CASSANDRA_PORT') ?: '9042');

        $nodes = [new ThrottledSocketNodeConfig(host: $host, port: $port, bytesPerSecond: $bytesPerSecond)];

        $conn = new Connection($nodes, self::COMPRESSION_KEYSPACE, new ConnectionOptions(enableCompression: $compress));
        $conn->setConsistency(Consistency::ONE);
        $conn->connect();

        return $conn;
    }

    /**
     * The deterministic, highly compressible blob used by the compression
     * benchmarks. Generated the same way everywhere so both the phpbench
     * benchmark and the standalone network report measure the same payload.
     */
    public static function compressionPayload(int $sizeBytes): string {
        $unit = 'The quick brown fox jumps over the lazy dog. 0123456789 ';

        return substr(str_repeat($unit, (int) ceil($sizeBytes / strlen($unit))), 0, $sizeBytes);
    }

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

    /**
     * Ensure the compression-benchmark keyspace, table and a single blob row of
     * the requested size exist. Idempotent and cheap on subsequent calls, so it
     * can run once per phpbench subprocess without re-uploading the payload.
     */
    public static function ensureCompressionFixture(int $sizeBytes): void {
        $host = getenv('APP_CASSANDRA_HOST') ?: '127.0.0.1';
        $port = (int) (getenv('APP_CASSANDRA_PORT') ?: '9042');
        $keyspace = self::COMPRESSION_KEYSPACE;

        $nodes = [new SocketNodeConfig(host: $host, port: $port, username: '', password: '')];

        $systemConn = new Connection($nodes, 'system');
        $systemConn->setConsistency(Consistency::ONE);
        $systemConn->connect();
        $systemConn->query("CREATE KEYSPACE IF NOT EXISTS {$keyspace} WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        $systemConn->disconnect();

        $conn = new Connection($nodes, $keyspace);
        $conn->setConsistency(Consistency::ONE);
        $conn->connect();
        $conn->query('CREATE TABLE IF NOT EXISTS docs (id int PRIMARY KEY, size int, data blob)');

        $existing = $conn->query('SELECT size FROM docs WHERE id = ?', [self::COMPRESSION_ROW_ID])->asRowsResult();
        $row = $existing->fetch();
        if (!is_array($row) || ($row['size'] ?? null) !== $sizeBytes) {
            $prepared = $conn->prepare('INSERT INTO docs (id, size, data) VALUES (?, ?, ?)');
            $conn->execute($prepared, [
                self::COMPRESSION_ROW_ID,
                $sizeBytes,
                new Blob(self::compressionPayload($sizeBytes)),
            ]);
        }

        $conn->disconnect();
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
