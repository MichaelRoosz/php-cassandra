Cassandra client library for PHP 
================================

Cassandra client library for PHP, which supports Protocol v5 (Cassandra 4.x) and asynchronous requests.

https://packagist.org/packages/mroosz/php-cassandra

## Features
* Using Protocol v5 (Cassandra 4.x)
* Supports ssl/tls with stream transport layer
* Supports asynchronous and synchronous requests
* Support for logged, unlogged and counter batches
* The ability to specify the consistency, "serial consistency" and all flags defined in the protocol
* Supports Query preparation and execute
* Supports all data types, including collection types, tuple and UDT
* Supports conditional update/insert
* 5 fetch methods (fetchAll, fetchRow, fetchPairs, fetchCol, fetchOne)
* Two transport layers - socket and stream.
* Uses exceptions to report errors
* 800% performance improvement(async mode) than other php cassandra client libraries

## Installation

PHP 8.1+ is required. There is no need for additional libraries.

If you want to use the Bigint, Counter, Duration, Time or Timestamp types, a 64-bit system is required.

Using composer to install is recommended.
```
composer require mroosz/php-cassandra
```

However, you may also fetch the repository from Github and load it via its own class loader:
```
require __DIR__ . '/php-cassandra/php-cassandra.php';
```

## Basic Usage

```php
<?php

$nodes = [
    '127.0.0.1',        // simple way, hostname only
    '192.168.0.2:9042', // simple way, hostname with port 
    [ // advanced way, array including username, password and socket options
        'host'        => '10.205.48.70',
        'port'        => 9042, //default 9042
        'username'    => 'admin',
        'password'    => 'pass',
        'socket'      => [SO_RCVTIMEO => ["sec" => 10, "usec" => 0], //socket transport only
        ],
    ],
    [ // advanced way, using Connection\Stream, persistent connection
        'host'        => '10.205.48.70',
        'port'        => 9042,
        'username'    => 'admin',
        'password'    => 'pass',
        'class'       => 'Cassandra\Connection\Stream',//use stream instead of socket, default socket. Stream may not work in some environment
        'connectTimeout' => 10, // connection timeout, default 5,  stream transport only
        'timeout'    => 30, // write/recv timeout, default 30, stream transport only
        'persistent'    => true, // use persistent PHP connection, default false,  stream transport only  
    ],
    [ // advanced way, using SSL/TLS
        'class'       => 'Cassandra\Connection\Stream', // "class" must be defined as "Cassandra\Connection\Stream" for ssl or tls
        'host'        => 'ssl://10.205.48.70',// or 'tls://10.205.48.70'
        'port'        => 9042,
        'username'    => 'admin',
        'password'    => 'pass',
        'ssl'        => ['verify_peer' => false, 'verify_peer_name' => false], // disable certificate verification
        //'ssl'        => ['cafile' => 'cassandra.pem', 'verify_peer_name'=>false] // with SSL certificate validation, no name check
    ],
];

// Create a connection.
$connection = new \Cassandra\Connection($nodes, 'my_keyspace');

//Connect
try
{
    $connection->connect();
}
catch (\Cassandra\Exception $e)
{
    echo 'Caught exception: ',  $e->getMessage(), "\n";
}


// Set consistency level for farther requests (default is CONSISTENCY_ONE)
$connection->setConsistency(Request::CONSISTENCY_QUORUM);

// Run query synchronously.
try
{
    $result = $connection->querySync('SELECT * FROM "users" WHERE "id" = ?', [new \Cassandra\Type\Uuid('c5420d81-499e-4c9c-ac0c-fa6ba3ebc2bc')]);
}
catch (\Cassandra\Exception $e)
{
}
```

## Fetch Data

```php
// Return a SplFixedArray containing all of the result set.
$rows = $result->fetchAll();   // SplFixedArray

// Return a SplFixedArray containing a specified index column from the result set.
$col = $result->fetchCol();    // SplFixedArray

// Return a assoc array with key-value pairs, the key is the first column, the value is the second column. 
$col = $result->fetchPairs();  // assoc array

// Return the first row of the result set.
$row = $result->fetchRow();    // ArrayObject

// Return the first column of the first row of the result set.
$value = $result->fetchOne();  // mixed
```

## Iterate over result
```php
// Print all roles
$result = $connection->querySync("SELECT role FROM system_auth.roles");
foreach($result AS $rowNo => $rowContent)
{
    echo $rowContent['role']."\n";
}
```

## Query Asynchronously

```php
// Return a statement immediately
try
{
    $statement1 = $connection->queryAsync($cql1);
    $statement2 = $connection->queryAsync($cql2);

    // Wait until received the result, can be reversed order
    $result2 = $statement2->getResult();
    $result1 = $statement1->getResult();


    $rows1 = $result1->fetchAll();
    $rows2 = $result2->fetchAll();
}
catch (\Cassandra\Exception $e)
{
}
```

## Using preparation and data binding

```php
$preparedData = $connection->prepare('SELECT * FROM "users" WHERE "id" = :id');

$strictValues = \Cassandra\Request\Request::strictTypeValues(
    [
        'id' => 'c5420d81-499e-4c9c-ac0c-fa6ba3ebc2bc',
    ],
    $preparedData['metadata']['columns']
);

$result = $connection->executeSync(
    $preparedData['id'],
    $strictValues,
    \Cassandra\Request\Request::CONSISTENCY_QUORUM,
    [
        'page_size' => 100,
        'names_for_values' => true,
        'skip_metadata' => true,
    ]
);

$result->setMetadata($preparedData['result_metadata']);
$rows = $result->fetchAll();
```

## Using Batch

```php
$batchRequest = new \Cassandra\Request\Batch();

// Append a prepared query
$preparedData = $connection->prepare('UPDATE "students" SET "age" = :age WHERE "id" = :id');
$values = [
    'age' => 21,
    'id' => 'c5419d81-499e-4c9c-ac0c-fa6ba3ebc2bc',
];
$batchRequest->appendQueryId($preparedData['id'], \Cassandra\Request\Request::strictTypeValues($values, $preparedData['metadata']['columns']));

// Append a query string
$batchRequest->appendQuery(
    'INSERT INTO "students" ("id", "name", "age") VALUES (:id, :name, :age)',
    [
        'id' => new \Cassandra\Type\Uuid('c5420d81-499e-4c9c-ac0c-fa6ba3ebc2bc'),
        'name' => new \Cassandra\Type\Varchar('Mark'),
        'age' => 20,
    ]
);

$result = $connection->batchSync($batchRequest);
$rows = $result->fetchAll();
```

## Supported datatypes

All types are supported.

```php
//  Ascii
    new \Cassandra\Type\Ascii('string');

//  Bigint
    new \Cassandra\Type\Bigint(10000000000);

//  Blob
    new \Cassandra\Type\Blob('string');

//  Boolean
    new \Cassandra\Type\Boolean(true);

//  Counter
    new \Cassandra\Type\Counter(1000);

//  Date
    \Cassandra\Type\Date::fromString('2011-02-03');
    \Cassandra\Type\Date::fromDateTime(new DateTimeImmutable('1970-01-01'));

    $date = new \Cassandra\Type\Date(19435);

    $dateAsString = $date->toString();
    $dateTime = $date->toDateTime();

//  Decimal
    new \Cassandra\Type\Decimal('0.0123');

//  Double
    new \Cassandra\Type\Double(2.718281828459);

//  Duration
    \Cassandra\Type\Duration::fromString('89h4m48s');
    \Cassandra\Type\Duration::fromDateInterval(new DateInterval('P6YT5M'));

    $duration = new \Cassandra\Type\Duration(['months' => 1, 'days' => 2, 'nanoseconds'=> 3]);

    $durationAsString = $duration->toString();

    // warning: loses nanosecond precision, DateInterval only supports microseconds
    $dateInterval = $duration->toDateInterval();

//  Float
    new \Cassandra\Type\PhpFloat(2.718);

//  Inet
    new \Cassandra\Type\Inet('127.0.0.1');

//  Int
    new \Cassandra\Type\PhpInt(12345678);

//  Smallint
    new \Cassandra\Type\Smallint(2048);

//  Tinyint
    new \Cassandra\Type\Tinyint(122);

//  CollectionList
    new \Cassandra\Type\CollectionList([1, 1, 1], [\Cassandra\Type::INT]);

//  CollectionMap
    new \Cassandra\Type\CollectionMap(['a' => 1, 'b' => 2], [\Cassandra\Type::ASCII, \Cassandra\Type::INT]);

//  CollectionSet
    new \Cassandra\Type\CollectionSet([1, 2, 3], [\Cassandra\Type::INT]);

//  Time (nanoseconds since midnight)
    \Cassandra\Type\Time::fromString('08:12:54.123456789');
    \Cassandra\Type\Time::fromDateTime(new DateTimeImmutable('08:12:54.123456789'));
    \Cassandra\Type\Time::fromDateInterval(new DateInterval('PT10H9M20S'));

    $time = new \Cassandra\Type\Time(18000000000000);

    $timeAsString = $time->toString();

    // warning: loses nanosecond precision, DateInterval only supports microseconds
    $dateInterval = $time->toDateInterval();

//  Timestamp
    \Cassandra\Type\Timestamp::fromString('2011-02-03T04:05:00.000+0000');
    \Cassandra\Type\Timestamp::fromDateTime(new DateTimeImmutable('2011-02-03T04:05:00.000+0000'))

    $timestamp1 = new \Cassandra\Type\Timestamp((int) (microtime(true) * 1000));
    $timestamp2 = new \Cassandra\Type\Timestamp(1409830696263);

    $timestampAsString = $timestamp1->toString();
    $dateTime = $timestamp2->toDateTime();

//  Uuid
    new \Cassandra\Type\Uuid('62c36092-82a1-3a00-93d1-46196ee77204');

//  Timeuuid
    new \Cassandra\Type\Timeuuid('2dc65ebe-300b-11e4-a23b-ab416c39d509');

//  Varchar
    new \Cassandra\Type\Varchar('string');

//  Varint
    new \Cassandra\Type\Varint(10000000000);

//  Custom
    new \Cassandra\Type\Custom('string', 'var_name');

//  Tuple
    new \Cassandra\Type\Tuple([1, '2'], [\Cassandra\Type::INT, \Cassandra\Type::VARCHAR]);

//  UDT
    new \Cassandra\Type\UDT([
        'intField' => 1, 
        'textField' => '2'
    ], [
        'intField' => \Cassandra\Type::INT,
        'textField' => \Cassandra\Type::VARCHAR
    ]); // in the order defined by the type
```

## Using nested datatypes

```php
// CollectionSet<UDT>, where UDT contains: Int, Text, Boolean, CollectionList<Text>, CollectionList<UDT>
new \Cassandra\Type\CollectionSet([
    [
        'id' => 1,
        'name' => 'string',
        'active' => true,
        'friends' => ['string1', 'string2', 'string3'],
        'drinks' => [['qty' => 5, 'brand' => 'Pepsi'], ['qty' => 3, 'brand' => 'Coke']]
    ],[
        'id' => 2,
        'name' => 'string',
        'active' => false,
        'friends' => ['string4', 'string5', 'string6'],
        'drinks' => []
    ]
], [
    [
        'type' => \Cassandra\Type::UDT,
        'definition' => [
            'id' => \Cassandra\Type::INT,
            'name' => \Cassandra\Type::VARCHAR,
            'active' => \Cassandra\Type::BOOLEAN,
            'friends' => [
                'type' => \Cassandra\Type::COLLECTION_LIST,
                'value' => \Cassandra\Type::VARCHAR
            ],
            'drinks' => [
                'type' => \Cassandra\Type::COLLECTION_LIST,
                'value' => [
                    'type' => \Cassandra\Type::UDT,
                    'typeMap' => [
                        'qty' => \Cassandra\Type::INT,
                        'brand' => \Cassandra\Type::VARCHAR
                    ]
                ]
            ]
        ]
    ]
]);
```

## Listening for events

```php
$connection->addEventListener(new class () implements \Cassandra\EventListener {
    public function onEvent(\Cassandra\Response\Event $event): void
    {
        var_dump($event->getData());
    }
});

$register = new \Cassandra\Request\Register([
    \Cassandra\Response\Event::TOPOLOGY_CHANGE,
    \Cassandra\Response\Event::STATUS_CHANGE,
    \Cassandra\Response\Event::SCHEMA_CHANGE,
]);

$connection->syncRequest($register);

while ($connection->getResponse()) {
    sleep(1);
}
```

## Inspired by
* [duoshuo/php-cassandra](https://github.com/duoshuo/php-cassandra)

## Merged contributions for duoshuo/php-cassandra
* https://github.com/arnaud-lb/php-cassandra/commit/b6444ee5f8f7079d7df80de85201b11f77e0d376
* https://github.com/duoshuo/php-cassandra/pull/78
* https://github.com/duoshuo/php-cassandra/pull/77
* https://github.com/duoshuo/php-cassandra/pull/66
