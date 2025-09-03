<?php

declare(strict_types=1);

use Cassandra\Connection;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Consistency;
use Cassandra\Type;

// Only attempt truncation when integration environment variables are present.
if (getenv('APP_CASSANDRA_HOST') === false && getenv('APP_CASSANDRA_PORT') === false) {
    return;
}

require __DIR__ . '/../vendor/autoload.php';

$host = getenv('APP_CASSANDRA_HOST') ?: '127.0.0.1';
$port = (int) (getenv('APP_CASSANDRA_PORT') ?: '9042');
$keyspace = getenv('APP_CASSANDRA_KEYSPACE') ?: 'app';

$nodes = [
    new SocketNodeConfig(
        host: $host,
        port: $port,
        username: '',
        password: ''
    ),
];

try {
    $connection = new Connection($nodes, $keyspace);
    $connection->setConsistency(Consistency::ONE);
    if (!$connection->connect()) {
        return;
    }

    // Fetch all base tables in the keyspace and truncate them
    $tablesResult = $connection->querySync(
        'SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?',
        [Type\Varchar::fromValue($keyspace)]
    )->asRowsResult();

    foreach ($tablesResult as $row) {
        $tableName = is_array($row) ? ($row['table_name'] ?? $row[0] ?? null) : null;
        if (!is_string($tableName) || $tableName === '') {
            continue;
        }
        // Using current keyspace, so simple table name is sufficient
        $connection->querySync('TRUNCATE ' . $tableName);
    }
} catch (\Throwable $_) {
    // Swallow errors to not break unit test runs or environments without Cassandra
}
