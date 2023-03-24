<?php

use Cassandra\Connection;
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

/*
var_dump((string)new Type\Counter(1234));

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
    'months' => -2147483648,
    'days' => -2147483648,
    'nanoseconds' => PHP_INT_MIN,
]));

var_dump((string)new Type\Duration([
    'months' => 2147483647,
    'days' => 2147483647,
    'nanoseconds' => PHP_INT_MAX,
]));
*/

#var_dump((string)Type\Duration::fromString('-1d2h10m'));

#var_dump((string)Type\Duration::fromString('-768614336404564650y8mo1317624576693539401w1d2562047h47m16s854ms775us808ns'));
#var_dump((string)Type\Duration::fromString('768614336404564650y7mo1317624576693539401w2562047h47m16s854ms775us807ns'));

#var_dump(Type\Duration::parse(Type\Duration::binary(['months' => 1, 'days' => 2, 'nanoseconds'=> 3])));
#var_dump(Type\Duration::parse(Type\Duration::binary(['months' => 223231, 'days' => 277756, 'nanoseconds'=> 320688000000000])));


#var_dump(Type\Duration::parse(Type\Duration::binary(['months' => 2147483647, 'days' => 2147483647, 'nanoseconds'=> PHP_INT_MAX])));
#var_dump(Type\Duration::parse(Type\Duration::binary(['months' => -2147483648, 'days' => -2147483648, 'nanoseconds'=> PHP_INT_MIN])));

#var_dump(Type\Varint::parse(Type\Varint::binary(PHP_INT_MAX)));
#var_dump(Type\Varint::parse(Type\Varint::binary(-1)));
#var_dump(Type\Varint::parse(Type\Varint::binary(-129)));
#var_dump(Type\Varint::parse(Type\Varint::binary(-5555)));
#var_dump(Type\Varint::parse(Type\Varint::binary(PHP_INT_MIN+1)));
#var_dump(Type\Varint::parse(Type\Varint::binary(PHP_INT_MIN)));
/*
var_dump((string)new Type\Duration([
    'months' => -2147483648,
    'days' => -2147483648,
    'nanoseconds' => PHP_INT_MIN,
]));

var_dump((string)new Type\Duration([
    'months' => 2147483647,
    'days' => 2147483647,
    'nanoseconds' => PHP_INT_MAX,
]));

var_dump(Type\Duration::toDateInterval(Type\Duration::fromString('-1d2h10m')->getValue())->format('%R %yY %mM %dD %hH %iM %sS %fF'));
var_dump(Type\Duration::toDateInterval(Type\Duration::fromString('1d2h10m')->getValue())->format('%R %yY %mM %dD %hH %iM %sS %fF'));

var_dump((string)(Type\Duration::fromString('-1d' . substr(PHP_INT_MIN, 1) . 'ns')));
var_dump('+' . (string)(Type\Duration::fromString('1d' . PHP_INT_MAX . 'ns')));

var_dump(Type\Duration::toDateInterval(Type\Duration::fromString('-1d' . substr(PHP_INT_MIN, 1) . 'ns')->getValue())->format('%R %yY %mM %dD %hH %iM %sS %fF'));
var_dump(Type\Duration::toDateInterval(Type\Duration::fromString('1d' . PHP_INT_MAX . 'ns')->getValue())->format('%R %yY %mM %dD %hH %iM %sS %fF'));

var_dump(Type\Duration::toDateInterval(Type\Duration::fromString('-1d' . substr((string)PHP_INT_MIN, 1) . 'ns')->getValue()));

var_dump(            '-178956970y8mo306783378w2d2562047h47m16s854ms775us808ns',
(string)new Type\Duration([
    'months' => -2147483648,
    'days' => -2147483648,
    'nanoseconds' => PHP_INT_MIN,
]));

var_dump(
    '- 184836873Y 5M 19D 23H 47M 16S 854775F',
    Type\Duration::toDateInterval(Type\Duration::fromString('-178956970y8mo306783378w2d2562047h47m16s854ms775us808ns')->getValue())->format('%R %yY %mM %dD %hH %iM %sS %fF')
);*/

$minDuration = [
    'months' => -2147483648,
    'days' => -2147483648,
    'nanoseconds' => PHP_INT_MIN,
];

var_dump(
    $minDuration,
    Type\Duration::fromDateInterval(Type\Duration::toDateInterval($minDuration))->getValue()
);

