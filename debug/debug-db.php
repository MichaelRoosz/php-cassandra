<?php

use Cassandra\Connection;
use Cassandra\Exception as CassandraException;
use Cassandra\Request\Request;
use Cassandra\Response\Result;
use Cassandra\Type;

require __DIR__ .  '/../php-cassandra.php';

$nodes = [
    [
        'host' => 'localhost',
        'port' => 8082,
        'username' => 'qr',
        'password' => 'qr',
        'class' => 'Cassandra\Connection\Stream',           //use stream instead of socket, default socket. Stream may not work in some environment
        'connectTimeout' => 10,                             // connection timeout, default 5,  stream transport only
        'timeout' => 30,                                    // write/recv timeout, default 30, stream transport only
        //'persistent' => true,                              // use persistent PHP connection, default false,  stream transport only
    ]
];

$keyspace = '';
$connection = new Connection($nodes, $keyspace);
$connection->connect();

// CREATE KEYSPACE IF NOT EXISTS test1 WITH replication = {'class': 'SimpleStrategy'};
// CREATE TABLE test1.test1 (id int, d duration, PRIMARY KEY (id));
// INSERT INTO test1.test1 (id, d) VALUES (1, 10h11m12s);
// INSERT INTO test1.test1 (id, d) VALUES (2, -10h11m12s);
// SELECT * FROM test1.test1;

var_dump((string)new Type\Duration([
    'months' => 1,
    'days' => 2,
    'nanoseconds' => 3,
]));

var_dump((string)new Type\Duration([
    'months' => -1,
    'days' => -2,
    'nanoseconds' => -3,
]));

var_dump((string)new Type\Duration([
    'months' => PHP_INT_MIN,
    'days' => PHP_INT_MIN,
    'nanoseconds' => PHP_INT_MIN,
]));

var_dump((string)new Type\Duration([
    'months' => PHP_INT_MAX,
    'days' => PHP_INT_MAX,
    'nanoseconds' => PHP_INT_MAX,
]));

var_dump((string)Type\Duration::fromString('-1d2h10m'));
var_dump((string)Type\Duration::fromString('-768614336404564650y8mo1317624576693539401w1d2562047h47m16s854ms775us808ns'));
var_dump((string)Type\Duration::fromString('1y1mo1w2d2562048h48m17s855ms776us808ns'));


