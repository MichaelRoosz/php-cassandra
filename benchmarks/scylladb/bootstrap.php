<?php

declare(strict_types=1);

/**
 * Bootstrap for ScyllaDB PHP driver benchmarks
 * This sets up the connection and ensures tables exist.
 */

// Environment defaults aligned with integration tests
if (!getenv('CASSANDRA_HOST')) {
    putenv('CASSANDRA_HOST=cassandra');
}
if (!getenv('CASSANDRA_PORT')) {
    putenv('CASSANDRA_PORT=9042');
}

final class ScyllaDBBenchEnv {
    public const DEFAULT_KEYSPACE = 'phpbenchks';
    private static $cluster = null;
    private static $session = null;

    public static function reset() {
        if (self::$session) {
            self::$session = null;
        }
        if (self::$cluster) {
            self::$cluster = null;
        }
    }

    public static function session() {
        if (self::$session !== null) {
            return self::$session;
        }

        $host = getenv('CASSANDRA_HOST') ?: 'cassandra';
        $port = (int) (getenv('CASSANDRA_PORT') ?: '9042');

        self::$cluster = \Cassandra::cluster()
            ->withContactPoints($host)
            ->withPort($port)
            ->withConnectTimeout(10)
            ->withRequestTimeout(10)
            ->build();

        // Retry connection with exponential backoff
        $maxRetries = 5;
        $retryDelay = 2;

        for ($attempt = 1; $attempt <= $maxRetries; $attempt++) {
            try {
                // First, connect without keyspace to create it
                $systemSession = self::$cluster->connect();
                self::ensureKeyspace($systemSession);
                unset($systemSession);

                // Then connect to the benchmark keyspace
                self::$session = self::$cluster->connect(self::DEFAULT_KEYSPACE);
                self::ensureTables(self::$session);

                echo "Connected to Cassandra successfully\n";

                return self::$session;
            } catch (\Cassandra\Exception\RuntimeException $e) {
                if ($attempt === $maxRetries) {
                    throw $e;
                }
                echo "Connection attempt {$attempt}/{$maxRetries} failed, retrying in {$retryDelay}s...\n";
                sleep($retryDelay);
                $retryDelay *= 2; // Exponential backoff
            }
        }

        return self::$session;
    }

    private static function ensureKeyspace($session) {
        $keyspace = self::DEFAULT_KEYSPACE;
        $query = "CREATE KEYSPACE IF NOT EXISTS {$keyspace} WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}";
        $session->execute($query, ['consistency' => \Cassandra::CONSISTENCY_ONE]);
    }

    private static function ensureTables($session) {
        $session->execute('CREATE TABLE IF NOT EXISTS kv (id int PRIMARY KEY, v text)', ['consistency' => \Cassandra::CONSISTENCY_ONE]);
        $session->execute('CREATE TABLE IF NOT EXISTS big_kv (filename varchar, ukey varchar, value map<varchar, varchar>, PRIMARY KEY (filename, ukey))', ['consistency' => \Cassandra::CONSISTENCY_ONE]);
    }
}
