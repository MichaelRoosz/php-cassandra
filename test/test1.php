<?php

declare(strict_types=1);

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
    'hosts' => ['cassandra_img'],
    'port' => 8092,
    'username' => 'api',
    'password' => 'api',
    'keyspace' => 'api',
];

$nodes = [];
foreach ($config['hosts'] as $h) {
    $nodes[] =[             // advanced way, using Connection\Stream, persistent connection
        'host'      => $h,
        'port'      => $config['port'],
        'username'  => $config['username'],
        'password'  => $config['password'],
        // 'class'     => 'Cassandra\Connection\Stream',//use stream instead of socket, default socket. Stream may not work in some environment
        'socket'      => [SO_RCVTIMEO => ['sec' => 10, 'usec' => 0]], //socket transport only
        'connectTimeout' => 10, // connection timeout, default 5,  stream transport only
        'timeout'   => 120, // write/recv timeout, default 30, stream transport only
        'persistent'    => true, // use persistent PHP connection, default false,  stream transport only
    ];
}

$connection = new Cassandra\Connection($nodes, $config['keyspace']);
$connection->setConsistency(Cassandra\Request\Request::CONSISTENCY_ONE);

$batches = [];
$testItem = json_decode('{
                        "id": "4003578",
                        "image": "https://example.music-retailer.com/media-dynamic/images/product/music/album/image0/christmas_by_the_bay-1001178-frntl.jpg",
                        "description": "null",
                        "title": "Christmas By The River (CD)",
                        "taxonomy": "Music > Pop > 80\'s artists"
                        }', true);

$batch = new Cassandra\Request\Batch(Cassandra\Request\Batch::TYPE_UNLOGGED, Cassandra\Request\Request::CONSISTENCY_ONE);

$counter = $i = $id = $total_counter = 0;
$partition= 1;
$start = microtime(true);

while (true) {
    $i++;
    $total_counter++;
    $counter++;
    $id++;

    $batch->appendQuery(
        'INSERT INTO storage(filename, ukey, value) VALUES(:filename,:ukey,:value)',
        [
            new Cassandra\Type\Varchar('p7_' . $partition),
            new Cassandra\Type\Varchar('key_' . $id),
            new Cassandra\Type\CollectionMap($testItem, [
                // [
                Cassandra\Type::VARCHAR,
                Cassandra\Type::VARCHAR,
                Cassandra\Type::VARCHAR,
                Cassandra\Type::VARCHAR,
                Cassandra\Type::VARCHAR,
                // ]
            ]),
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
        $batch = new Cassandra\Request\Batch(Cassandra\Request\Batch::TYPE_UNLOGGED, Cassandra\Request\Request::CONSISTENCY_ONE);
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
$start = microtime(true);
foreach ($batches as $batch) {
    usleep(500);
    $response = $connection->asyncRequest($batch);
}

$taken = round(microtime(true) - $start,2);
echo "taken: $taken | items: $counter ( total: $i) | partition: $partition | Cass Ops/sec: " .
    round($total_counter / $taken) . ' | ' . 'memory used : ' .
    round((memory_get_usage()/1024)/1024) . "MB\n";

// 1

$preparedData = $connection->prepare('SELECT * FROM storage where filename = :filename');
$result = $connection->executeSync(
    $preparedData,
    [
        'filename' => 'p7_2',
    ],
    \Cassandra\Request\Request::CONSISTENCY_QUORUM,
    [
        'page_size' => 100,
        'names_for_values' => true,
    ]
);

$state = $result->getMetadata()['paging_state'] ?? false;
echo json_encode($result->fetchRow()) . "\n" . $state . "\n\n";

// == Q2
$result = $connection->executeSync(
    $result,
    [
        'filename' => 'p7_2',
    ],
    \Cassandra\Request\Request::CONSISTENCY_QUORUM,
    [
        'page_size' => 100,
        'names_for_values' => true,
        'paging_state' => $state,
    ]
);

$state = $result->getMetadata()['paging_state'] ?? false;
echo json_encode($result->fetchRow()) . "\n" . $state . "\n\n";

// 2

$query = "SELECT * FROM storage where filename = 'p7_2'";
$statement1 = $connection->querySync($query,
    [],
    Cassandra\Request\Request::CONSISTENCY_ONE,
    [
        'page_size' => 100,
    ]
);

// == Q2
$state = $statement1->getMetadata()['paging_state'] ?? false;
echo json_encode($statement1->fetchRow()) . "\n" . $state . "\n\n";

$statement1 = $connection->querySync($query,
    [],
    Cassandra\Request\Request::CONSISTENCY_ONE,
    [
        'page_size' => 100,
        'paging_state' => $state,
    ]
);

$state = $statement1->getMetadata()['paging_state'] ?? false;
echo json_encode($statement1->fetchRow()) . "\n" . $state . "\n\n";
