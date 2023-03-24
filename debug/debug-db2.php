<?php

require __DIR__ .  '/../php-cassandra.php';

use Cassandra\Connection;
use Cassandra\Type;

$nodes =[
    [
        'host' => '127.0.0.1',
        'port' => 8082,
        'username' => 'qr',
        'password' =>'qr',
        'class' => 'Cassandra\Connection\Stream',           //use stream instead of socket, default socket. Stream may not work in some environment
        'connectTimeout' => 10,                             // connection timeout, default 5,  stream transport only
        'timeout' => 30,                                    // write/recv timeout, default 30, stream transport only
        'persistent' => false,                              // use persistent PHP connection, default false,  stream transport only
    ]
];

$keyspace = '';
$connection = new Connection($nodes, $keyspace, ['COMPRESSION' => 'lz4']);
$connection->connect();

// CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

// CREATE TABLE test.test1 ( id VARCHAR PRIMARY KEY, name TEXT, int1 TINYINT );
// INSERT INTO test.test1 (id,name,int1) VALUES('6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47',  'KRUIJSWIJK',  123);
// SELECT * FROM test.test1;

/*
CREATE TABLE test.test2 ( id VARCHAR PRIMARY KEY,
a ascii,
b bigint,
c blob,
d boolean,
f date,
g decimal,
h double,
i duration,
j float,
k inet,
l int,
m smallint,
n text,
o time,
p timestamp,
q timeuuid,
r tinyint,
s uuid,
t varchar,
u varint,
v tuple<int, text>,
);
*/

/*
INSERT INTO test.test2 (id,
a,
b,
c,
d,
f,
g,
h,
i,
j,
k,
l,
m,
n,
o,
p,
q,
r,
s,
t,
u,
v
)
VALUES('6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47',
'abcABC123!#_',
9223372036854775807,
textAsBlob('xxxxxyyyyyy'),
true,
'2011-02-03',
34345454545.120,
12345678901234.4545435,
89h4m48s,
1024.5,
'192.168.22.1',
-234355434,
32123,
'abcABC123!#_TEXT',
'08:12:54.123456789',
'2011-02-03T04:05:00.000+0000',
bd23b48a-99de-11ed-a8fc-0242ac120002,
-127,
346c9059-7d07-47e6-91c8-092b50e8306f,
'abcABC123!#_VAR',
922337203685477580,
(3, 'hours')
);
*/
// SELECT * FROM test.test2;


$result1 = $connection->querySync('SELECT * FROM test.test1');
$data1 = $result1->fetchAll();

var_dump($data1);

var_dump([
    #(string)\Cassandra\Type\Date::fromString('2011-02-03'),
    #(string)\Cassandra\Type\Date::fromDateTime(new DateTimeImmutable('1970-01-01')),
    #(string)new \Cassandra\Type\Date(19435),

    #(string)\Cassandra\Type\Duration::fromString('89h4m48s'),

    #(string)\Cassandra\Type\Time::fromString('08:12:54.123456789'),
    #(string)\Cassandra\Type\Time::fromDateTime(new DateTimeImmutable('08:12:54.123456789')),
    #(string)\Cassandra\Type\Time::fromDateInterval(new DateInterval('PT10H9M20S')),
    #(string)new \Cassandra\Type\Time(18000000000000),
    #\Cassandra\Type\Time::toString(18000000000000),
    #\Cassandra\Type\Time::toDateInterval(18000000000000)->format('%yY %mM %dD %hH %iM %sS %fF'),
]);

#var_dump((string)(new Type\Tinyint(1024)));

#var_dump(Type\Time::toString($data1[0]['o']));
#var_dump(Type\Time::toDateInterval($data1[0]['o'])->format(' %yY %mM %dD %hH %iM %sS %fF'));
#var_dump((string)Type\Time::fromString('08:12:54.123456789'));
#var_dump((string)Type\Time::fromDateTime(new DateTimeImmutable('08:12:54.123456789')));
#

/*
var_dump(Type\Date::toString($data1[0]['f']));
var_dump((string)Type\Date::fromString('2011-02-03'));

var_dump(Type\Time::toString($data1[0]['o']));
var_dump((string)Type\Time::fromString('08:12:54.123456789'));
var_dump((string)Type\Time::fromDateTime(new DateTimeImmutable('08:12:54.123456789')));

var_dump(Type\Timestamp::toString($data1[0]['p']));
var_dump((string)Type\Timestamp::fromString('2011-02-03T04:05:00.000+0000'));
*/

#var_dump(Type\Duration::parse(Type\Duration::binary(['months' => 1, 'days' => 2, 'nanoseconds'=> 3])));
#var_dump(Type\Duration::parse(Type\Duration::binary(['months' => 223231, 'days' => 277756, 'nanoseconds'=> 320688000000000])));
#var_dump(Type\Duration::toString($data1[0]['i']));
#var_dump((string)Type\Duration::fromString('89h4m48s'));
#var_dump((string)Type\Duration::fromString('1y2mo3w4d5h6m7s8ms9us10ns'));
#var_dump(Type\Duration::toDateInterval(Type\Duration::fromDateInterval(new DateInterval('P10Y11M12DT89H4M48S'))->getValue())->format(' %yY %mM %dD %hH %iM %sS %fF'));
#var_dump(Type\Duration::toDateInterval(Type\Duration::fromString('1y2mo3w4d5h6m7s8ms9us10ns')->getValue())->format(' %yY %mM %dD %hH %iM %sS %fF'));

#var_dump((string)Type\Duration::fromDateInterval(new DateInterval('PT89H4M48S')));

#var_dump(measure_execution_time2());

function measure_execution_time(int $loops = 1000000)
{
    $original = [];
    $binary = [];

    for ($i = 0; $i < $loops; $i++) {
        $v = random_int(0, PHP_INT_MAX);
        $original[$i] = $v;
    }

    $start_time_function1 = microtime(true);
    for ($i = 0; $i < $loops; $i++) {
        $binary[$i] = Type\Duration::encodeVint($original[$i]);
    }
    $end_time_function1 = microtime(true);
    $execution_time_function1 = $end_time_function1 - $start_time_function1;

    for ($i = 0; $i < $loops; $i++) {
        $binary[$i] = array_values(unpack('C*', $binary[$i]));
    }

    /*
    $start_time_function2 = microtime(true);
    for ($i = 0; $i < $loops; $i++) {
        $binary2[$i] = Type\Duration::encodeVint2($original[$i]);
    }
    $end_time_function2 = microtime(true);
    $execution_time_function2 = $end_time_function2 - $start_time_function2;


    $start_time_function3 = microtime(true);
    for ($i = 0; $i < $loops; $i++) {
        $binary3[$i] = Type\Duration::encodeVint3($original[$i]);
    }
    $end_time_function3 = microtime(true);
    $execution_time_function3 = $end_time_function3 - $start_time_function3;

    for ($i = 0; $i < $loops; $i++) {
        if ($binary[$i] !== $binary3[$i]) {
            throw new Exception('failed: ' . var_export(['binary' => tobin($binary[$i]) ,  'binary3' => tobin($binary3[$i]) ], true));
        }
    }

    var_dump([
        'execution_time_function1' => $execution_time_function1,
        'execution_time_function2' => $execution_time_function2,
        'execution_time_function3' => $execution_time_function3,
    ]);
    */
    // Measure the execution time of the first function
    $output1 = [];
    $start_time_function1 = microtime(true);
    for ($i = 0; $i < $loops; $i++) {
        $output1[$i] = Type\Duration::decodeVint($binary[$i]);
        if ($original[$i] !== $output1[$i]) {
            throw new Exception('failed: ' . var_export(['original' => $original[$i] ,  'output1' => $output1[$i] ], true));
        };
    }
    $end_time_function1 = microtime(true);
    $execution_time_function1 = $end_time_function1 - $start_time_function1;

    // Measure the execution time of the second function
    $output2 = [];
    $start_time_function2 = microtime(true);
    for ($i = 0; $i < $loops; $i++) {
        $output2[$i] = Type\Duration::decodeVint($binary[$i]);
        if ($original[$i] !== $output2[$i]) {
            throw new Exception('failed: ' . var_export(['original' => $original[$i] ,  'output2' => $output2[$i] ], true));
        }
    }
    $end_time_function2 = microtime(true);
    $execution_time_function2 = $end_time_function2 - $start_time_function2;

    // Compare the results
    $outputs_are_equal = ($output1 === $output2);

    return [
        'execution_time_function1' => $execution_time_function1,
        'execution_time_function2' => $execution_time_function2,
        'outputs_are_equal' => $outputs_are_equal,
       // 'outputs_diff' => array_diff($output1, $output2),
    ];
}

function measure_execution_time2(int $loops = 1000000)
{
    $original = [];

    for ($i = 0; $i < $loops; $i++) {
        $v = random_int(PHP_INT_MIN, PHP_INT_MAX);
        $original[$i] = $v;
    }

    // Measure the execution time of the first function
    $output1 = [];
    $start_time_function1 = microtime(true);
    for ($i = 0; $i < $loops; $i++) {
        $output1[$i] = Type\Bigint::parse(Type\Bigint::binary($original[$i]));
        if ($original[$i] !== $output1[$i]) {
            throw new Exception('failed: ' . var_export(['original' => $original[$i] ,  'output1' => $output1[$i] ], true));
        };
    }
    $end_time_function1 = microtime(true);
    $execution_time_function1 = $end_time_function1 - $start_time_function1;

    // Measure the execution time of the second function
    $output2 = [];
    $start_time_function2 = microtime(true);
    for ($i = 0; $i < $loops; $i++) {
        $output2[$i] = Type\Bigint::parse2(Type\Bigint::binary2($original[$i]));
        if ($original[$i] !== $output2[$i]) {
            throw new Exception('failed: ' . var_export(['original' => $original[$i] ,  'output2' => $output2[$i] ], true));
        }
    }
    $end_time_function2 = microtime(true);
    $execution_time_function2 = $end_time_function2 - $start_time_function2;

    // Compare the results
    $outputs_are_equal = ($output1 === $output2);

    return [
        'execution_time_function1' => $execution_time_function1,
        'execution_time_function2' => $execution_time_function2,
        'outputs_are_equal' => $outputs_are_equal,
        'outputs_diff' => array_diff($output1, $output2),
    ];
}

function tobin($data)
{
    $bin = '';
    foreach (str_split($data) as $byte) {
        $bin .= ' ' . str_pad(decbin(ord($byte)), 8, '0', STR_PAD_LEFT);
    }

    return $bin;
}
