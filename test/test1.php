<?php

declare(strict_types=1);

use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\Consistency;
use Cassandra\Request\BatchType;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Type;

require __DIR__ . '/../vendor/autoload.php';

/*
 CREATE TABLE api.storage (
     filename text,
     ukey text,
     value map<text, text>,
     PRIMARY KEY (filename, ukey)
 ) WITH CLUSTERING ORDER BY (ukey ASC);
*/

$config = [
    'host' => 'cassandra_img',
    'port' => 8092,
    'username' => 'api',
    'password' => 'api',
    'keyspace' => 'api',
];

$nodes = [
    new SocketNodeConfig(
        host: $config['host'],
        port: $config['port'],
        username: $config['username'],
        password: $config['password'],
        socketOptions: [
            SO_RCVTIMEO => ['sec' => 10, 'usec' => 0],
            SO_SNDTIMEO => ['sec' => 10, 'usec' => 0],
        ],
    ),
    new StreamNodeConfig(
        host: $config['host'],
        port: $config['port'],
        username: $config['username'],
        password: $config['password'],
        connectTimeoutInSeconds: 10,
        timeoutInSeconds: 120,
        persistent: true,
    ),
];

$connection = new Cassandra\Connection($nodes, $config['keyspace']);
$connection->setConsistency(Consistency::ONE);

$batches = [];
$testItem = json_decode('{
                        "id": "4003578",
                        "image": "https://example.music-retailer.com/media-dynamic/images/product/music/album/image0/christmas_by_the_bay-1001178-frntl.jpg",
                        "description": "null",
                        "title": "Christmas By The River (CD)",
                        "taxonomy": "Music > Pop > 80\'s artists"
                        }', true);

$batch = new Cassandra\Request\Batch(BatchType::UNLOGGED, Consistency::ONE);

$counter = $i = $id = $total_counter = 0;
$partition= 1;
$start = microtime(true);

while (true) {
    $i++;
    $total_counter++;
    $counter++;
    $id++;

    $batch->appendQuery(
        'INSERT INTO storage(filename, ukey, value) VALUES(:filename, :ukey, :value)',
        [
            new Cassandra\Type\Varchar('p7_' . $partition),
            new Cassandra\Type\Varchar('key_' . $id),
            new Cassandra\Type\CollectionMap($testItem, Type::VARCHAR, Type::VARCHAR),
        ]
    );

    if (microtime(true) - $start > 5) {
        $taken = round(microtime(true) - $start,2);
        $start = microtime(true);

        echo "taken: $taken | items: $counter ( total: $i) | partition: $partition | PHP Ops/sec: " .
            round($total_counter / $taken) . ' | ' . 'memory used : ' .
            round((memory_get_usage()/1024)/1024) . "MB\n";
        $counter = 0;
    }

    if ($counter > 200) {
        $batches[] = $batch;
        $batch = new Cassandra\Request\Batch(BatchType::UNLOGGED, Consistency::ONE);
        $counter = 0;
    }

    if ($id >= 100000) {
        $partition++;
        $id = 0;
    }

    if ($i >= 200000) {
        break;
    }
}

echo count($batches) . "\n";
$batchStatements = [];
$start = microtime(true);
foreach ($batches as $batch) {
    usleep(500);
    $batchStatements[] = $connection->batchAsync($batch);
}

foreach ($batchStatements as $statement) {
    $statement->waitForResponse();
}

$taken = round(microtime(true) - $start,2);
echo "taken: $taken | items: $counter ( total: $i) | partition: $partition | Cass Ops/sec: " .
    round($total_counter / $taken) . ' | ' . 'memory used : ' .
    round((memory_get_usage()/1024)/1024) . "MB\n";

// 1
$preparedData = $connection->prepareSync('SELECT * FROM storage where filename = :filename');
$result = $connection->executeSync(
    $preparedData,
    [
        'filename' => 'p7_2',
    ],
    Consistency::QUORUM,
    new ExecuteOptions(
        pageSize: 100,
        namesForValues: true,
    )
)->asRowsResult();

$state = $result->getMetadata()->pagingState;
echo json_encode($result->fetch()) . "\n" . $state . "\n\n";

// == Q2
$result = $connection->executeSync(
    $result,
    [
        'filename' => 'p7_2',
    ],
    Consistency::QUORUM,
    new ExecuteOptions(
        pageSize: 100,
        namesForValues: true,
        pagingState: $state,
    )
)->asRowsResult();

$state = $result->getMetadata()->pagingState;
echo json_encode($result->fetch()) . "\n" . $state . "\n\n";

// 2
$query = "SELECT * FROM storage where filename = 'p7_2'";
$statement1 = $connection->querySync($query,
    [],
    Consistency::ONE,
    new QueryOptions(
        pageSize: 100,
    )
)->asRowsResult();

// == Q2
$state = $statement1->getMetadata()->pagingState;
echo json_encode($statement1->fetch()) . "\n" . $state . "\n\n";

$statement1 = $connection->querySync($query,
    [],
    Consistency::ONE,
    new QueryOptions(
        pageSize: 100,
        pagingState: $state,
    )
)->asRowsResult();

$state = $statement1->getMetadata()->pagingState;
echo json_encode($statement1->fetch()) . "\n" . $state . "\n\n";
