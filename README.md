php-cassandra: A modern Cassandra client for PHP
================================================

php-cassandra is a pure-PHP client for Apache Cassandra with support for CQL binary protocol v3, v4 and v5 (Cassandra 4.x/5.x), synchronous and asynchronous APIs, prepared statements, batches, result iterators, object mapping, SSL/TLS, and LZ4 compression.

Package: https://packagist.org/packages/mroosz/php-cassandra

Table of contents
-----------------

- Introduction
- Requirements
- Installation
- Quick start
- Connecting (sockets vs streams, TLS, options)
- Consistency levels
- Queries (sync/async, options, auto-prepare)
- Asynchronous API
- Prepared statements (named vs positional, keyspace v5, pagination)
- Batches
- Results and fetching (iterators, fetch styles, object mapping)
- Data types
- Type definition syntax for complex values
- Events
- Tracing and custom payloads
- Compression
- Error handling
- Key features and options
- Best practices and tuning
- Troubleshooting
 - Error-handling matrix
 - Type mapping reference
- API reference (essentials)
- Running tests
- License and Credits

Introduction
------------

This library focuses on correctness, protocol coverage, and a pragmatic developer experience:

- Protocol v3/v4/v5, auto-negotiated at connect time
- Two transports: sockets and PHP streams (streams support SSL/TLS and persistent connections)
- Synchronous and asynchronous requests, with efficient pipelining
- Prepared statements with positional or named binding; query auto-prepare for PHP scalars
- Batches: logged, unlogged, counter
- Full data type coverage (collections, tuples, UDTs, custom, vectors)
- Iterators, multiple fetch styles, and object mapping
- Events (schema/status/topology) with a simple listener interface
- Optional LZ4 compression and server overload signalling

Requirements
------------

- PHP 8.1+
- 64-bit PHP for 64-bit types like Bigint, Counter, Duration, Time, Timestamp
- For socket transport: PHP sockets extension; streams require no extra extensions

Installation
------------

Using Composer:
```bash
composer require mroosz/php-cassandra
```

Or load the library without Composer:
```php
require __DIR__ . '/php-cassandra/php-cassandra.php';
```

Quick start
-----------

```php
<?php

use Cassandra\Connection;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Consistency;

// Choose one or more nodes and a transport
$nodes = [
    new SocketNodeConfig(host: '127.0.0.1', port: 9042, username: 'cassandra', password: 'cassandra'),
    // or streams (supports SSL/TLS and persistent connections)
    // new StreamNodeConfig(host: '127.0.0.1', port: 9042, username: 'cassandra', password: 'cassandra'),
];

// Optional connection options
$options = new ConnectionOptions(
    enableCompression: true,   // LZ4 if server supports it
    throwOnOverload: true,     // protocol v4+
);

$conn = new Connection($nodes, keyspace: 'my_keyspace', options: $options);
$conn->connect();

// Consistency default for subsequent requests
$conn->setConsistency(Consistency::QUORUM);

// Plain query (positional bind)
$rows = $conn->query(
    'SELECT * FROM ks.users WHERE id = ?',
    [\Cassandra\Value\Uuid::fromValue('c5420d81-499e-4c9c-ac0c-fa6ba3ebc2bc')]
)->asRowsResult()->fetchAll();

// Prepared statement (named bind + paging)
$prepared = $conn->prepare('SELECT id, name FROM ks.users WHERE org_id = :org_id');

$result = $conn->execute(
    $prepared,
    values: ['org_id' => 42],
    consistency: Consistency::LOCAL_QUORUM,
    options: new \Cassandra\Request\Options\ExecuteOptions(
        pageSize: 100,
        namesForValues: true
    )
)->asRowsResult();

foreach ($result as $row) {
    echo $row['name'], "\n";
}
```

Connecting
----------

Create `NodeConfig` instances and pass them to `Connection`:

```php
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\Connection;

$socketNode = new SocketNodeConfig(
    host: '10.0.0.10',
    port: 9042,
    username: 'user',
    password: 'secret',
    socketOptions: [SO_RCVTIMEO => ['sec' => 10, 'usec' => 0]]
);

// Streams transport with SSL/TLS and persistent connection
$streamTlsNode = new StreamNodeConfig(
    host: 'tls://cassandra.example.com',
    port: 9042,
    username: 'user',
    password: 'secret',
    connectTimeoutInSeconds: 10,
    timeoutInSeconds: 30,
    persistent: true,
    sslOptions: [
        // See https://www.php.net/manual/en/context.ssl.php
        'cafile' => '/etc/ssl/certs/ca.pem',
        'verify_peer' => true,
        'verify_peer_name' => true,
    ]
);

$conn = new Connection([$socketNode, $streamTlsNode], keyspace: 'app');
$conn->connect();
```

Connection options are provided via `ConnectionOptions`:
- `enableCompression` = use LZ4 if enabled on server
- `throwOnOverload` = 'true' to ask server to throw on overload (v4+)
- `nodeSelectionStrategy` = `Random` (default) or `RoundRobin`
- `preparedResultCacheSize` = cache size for prepared metadata (default 100)

Keyspace selection:
- v5: can also be sent per-request via Query/Execute options (see below)
- v3/v4: call `$conn->setKeyspace('ks')` or run `USE ks`

Consistency levels
------------------

Use the `Consistency` enum:
- `ALL`, `ANY`, `EACH_QUORUM`, `LOCAL_ONE`, `LOCAL_QUORUM`, `LOCAL_SERIAL`, `ONE`, `QUORUM`, `SERIAL`, `THREE`, `TWO`

Apply per call or as default via `setConsistency()`.

Queries
-------

Synchronous:
```php
use Cassandra\Value\Uuid;
use Cassandra\Consistency;
use Cassandra\Request\Options\QueryOptions;

$rowsResult = $conn->query(
    'SELECT id, name FROM ks.users WHERE id = ?',
    [Uuid::fromValue($id)],
    consistency: Consistency::ONE,
    options: new QueryOptions(pageSize: 100)
)->asRowsResult();
```

Asynchronous:
```php
use Cassandra\Request\Options\QueryOptions;

$s1 = $conn->queryAsync('SELECT count(*) FROM ks.t1', options: new QueryOptions(pageSize: 1000));
$s2 = $conn->queryAsync('SELECT count(*) FROM ks.t2', options: new QueryOptions(pageSize: 1000));

// Option A: wait individually
$r2 = $s2->getResult()->asRowsResult();
$r1 = $s1->getResult()->asRowsResult();
```

Query options (`QueryOptions`):
- `autoPrepare` (bool, default true): transparently prepare+execute when needed
- `pageSize` (int, min 100 enforced by client)
- `pagingState` (string)
- `serialConsistency` (`SerialConsistency::SERIAL` or `SerialConsistency::LOCAL_SERIAL`)
- `defaultTimestamp` (ms since epoch)
- `namesForValues` (bool): true to use associative binds; if not explicitly set, it is auto-detected for queries and executes
- `keyspace` (string; protocol v5 only)
- `nowInSeconds` (int; protocol v5 only)

Notes:
- If you supply non-`Value\*` PHP values with `QueryOptions(autoPrepare: true)`, the driver auto-prepares + executes for correct typing.
- Always use fully-qualified table names (including keyspace) for `PREPARE` statements to avoid ambiguity, e.g. `SELECT ... FROM ks.users WHERE ...`.

Fetch all pages helpers:
```php
// For simple queries
$pages = $conn->queryAll('SELECT * FROM ks.users WHERE org_id = ?', [$orgId]);
foreach ($pages as $page) {
    foreach ($page as $row) {
        // ...
    }
}
```

Prepared statements
-------------------

```php
use Cassandra\Request\Options\ExecuteOptions;

$prepared = $conn->prepare('SELECT * FROM ks.users WHERE email = :email');

$rowsResult = $conn->execute(
    $prepared,
    ['email' => 'jane@example.com'],
    options: new ExecuteOptions(
        namesForValues: true,
        pageSize: 50
    )
)->asRowsResult();
```

Pagination with prepared statements:
```php
use Cassandra\Request\Options\ExecuteOptions;

$options = new ExecuteOptions(pageSize: 100, namesForValues: true);
$result = $conn->execute($prepared, ['org_id' => 1], options: $options)->asRowsResult();

do {
    foreach ($result as $row) {
        // process row
    }

    $pagingState = $result->getRowsMetadata()->pagingState;
    if ($pagingState === null) break;

    $options = new ExecuteOptions(
        pageSize: 100,
        namesForValues: true,
        pagingState: $pagingState
    );
    $result = $conn->execute($result, [], options: $options)->asRowsResult(); // reuse previous RowsResult for metadata id
} while (true);
```

Execute all pages helper:
```php
use Cassandra\Request\Options\ExecuteOptions;

$pages = $conn->executeAll($prepared, ['org_id' => 1], options: new ExecuteOptions(namesForValues: true));
```

Additional notes:
- For `PREPARE` and `EXECUTE`, `namesForValues` is auto-detected if not set explicitly based on the array keys (associative vs indexed).
- Always use fully-qualified table names in prepared statements.

Batches
-------

```php
use Cassandra\Consistency;
use Cassandra\Request\Batch;
use Cassandra\Request\BatchType;
use Cassandra\Value\Uuid;
use Cassandra\Value\Varchar;

$batch = new Batch(type: BatchType::LOGGED, consistency: Consistency::QUORUM);

// Prepared in batch (namesForValues: use associative array)
$prepared = $conn->prepare('UPDATE ks.users SET age = :age WHERE id = :id');
$batch->appendPreparedStatement($prepared, ['age' => 21, 'id' => 'c5419d81-499e-4c9c-ac0c-fa6ba3ebc2bc']);

// Simple query in batch (positional)
$batch->appendQuery(
    'INSERT INTO ks.users (id, name, age) VALUES (?, ?, ?)',
    [
        Uuid::fromValue('c5420d81-499e-4c9c-ac0c-fa6ba3ebc2bc'),
        Varchar::fromValue('Mark'),
        20,
    ]
);

$conn->batch($batch);
```

Batch notes:
- BATCH does not support names for values at the protocol level for simple queries; use positional values for `appendQuery`. For prepared entries, provide values consistent with the prepared statement (associative for named markers).
- `BatchOptions`: `serialConsistency`, `defaultTimestamp`, `keyspace` (v5), `nowInSeconds` (v5).

Results and fetching
--------------------

`query()`/`execute()` return a `Result`; call `asRowsResult()` for row-returning queries. Supported `RowsResult` methods:
- `fetch(FetchType::ASSOC|NUM|BOTH)` returns next row or false
- `fetchAll(FetchType)` returns all remaining rows
- `fetchColumn(int $index)` / `fetchAllColumns(int $index)`
- `fetchKeyPair(int $keyIndex, int $valueIndex)` / `fetchAllKeyPairs(...)`
- `getIterator()` returns a `ResultIterator` so you can `foreach ($rowsResult as $row)`

Example:
```php
use Cassandra\Response\Result\FetchType;

$r = $conn->query('SELECT role FROM system_auth.roles')->asRowsResult();
foreach ($r as $i => $row) {
    echo $row['role'], "\n";
}

$names = $r->fetchAllColumns(0); // remaining rows of first column
```

Object mapping
--------------

You can fetch rows into objects by implementing `RowClassInterface` or by using the default `RowClass`:

```php
use Cassandra\Response\Result\RowClassInterface;

final class UserRow implements RowClassInterface {
    public function __construct(private array $row, array $args = []) {}
    public function id(): string { return (string) $this->row['id']; }
    public function name(): string { return (string) $this->row['name']; }
}

$rows = $conn->query('SELECT id, name FROM ks.users')->asRowsResult();
$rows->configureFetchObject(UserRow::class);

foreach ($rows as $user) {
    echo $user->name(), "\n";
}
```

Data types
----------

All native Cassandra types are supported via classes in `Cassandra\Value\*`. You may pass either:
- A concrete `Value\...` instance, or
- A PHP scalar/array matching the type; the driver will convert it when metadata is available

Examples:
```php
use Cassandra\Value\Ascii;
use Cassandra\Value\Bigint;
use Cassandra\Value\Boolean;
use Cassandra\Value\Double;
use Cassandra\Value\Float32;
use Cassandra\Value\Int32;
use Cassandra\Value\Smallint;
use Cassandra\Value\Tinyint;
use Cassandra\Value\Varint;
use Cassandra\Value\Date;
use Cassandra\Value\Time;
use Cassandra\Value\Timestamp;
use Cassandra\Value\Duration;
use Cassandra\Value\ListCollection;
use Cassandra\Value\SetCollection;
use Cassandra\Value\MapCollection;
use Cassandra\Value\Tuple;
use Cassandra\Value\UDT;
use Cassandra\Value\Vector;
use Cassandra\Type;

// Scalars
Ascii::fromValue('hello');
Bigint::fromValue(10_000_000_000);
Boolean::fromValue(true);
Double::fromValue(2.718281828459);
Float32::fromValue(2.718);
Int32::fromValue(123);
Smallint::fromValue(2048);
Tinyint::fromValue(12);
Varint::fromValue(10000000000);

// Temporal
Date::fromValue('2011-02-03');
Time::fromValue('08:12:54.123456789');
Timestamp::fromValue('2011-02-03T04:05:00.000+0000');
Duration::fromValue('89h4m48s');

// Collections / Tuples / UDT / Vector
ListCollection::fromValue([1, 2, 3], [Type::INT]);
SetCollection::fromValue([1, 2, 3], [Type::INT]);
MapCollection::fromValue(['a' => 1], Type::ASCII, Type::INT);
Tuple::fromValue([1, 'x'], [Type::INT, Type::VARCHAR]);
UDT::fromValue(['id' => 1, 'name' => 'n'], ['id' => Type::INT, 'name' => Type::VARCHAR]);
Vector::fromValue([0.12, -0.3, 0.9]);
```

Type definition syntax for complex values
----------------------------------------

For complex types, the driver needs a type definition to encode PHP values. Wherever you see a parameter like `\Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>)`, you can either pass a scalar `Type::...` (for simple elements) or a definition array with nested types for complex structures. The common shapes are:

- List: `['type' => Type::LIST, 'valueType' => <elementType>, 'isFrozen' => bool]`
- Set: `['type' => Type::SET, 'valueType' => <elementType>, 'isFrozen' => bool]`
- Map: `['type' => Type::MAP, 'keyType' => <keyType>, 'valueType' => <valueType>, 'isFrozen' => bool]`
- Tuple: `['type' => Type::TUPLE, 'valueTypes' => [<t1>, <t2>, ...]]`
- UDT: `['type' => Type::UDT, 'valueTypes' => ['field' => <type>, ...], 'isFrozen' => bool, 'keyspace' => 'ks', 'name' => 'udt_name']`

Examples
```php
use Cassandra\Type;
use Cassandra\Value\ListCollection;
use Cassandra\Value\SetCollection;
use Cassandra\Value\MapCollection;
use Cassandra\Value\Tuple;
use Cassandra\Value\UDT;

// List<int>
ListCollection::fromValue([1,2,3], Type::INT);

// Set<text>
SetCollection::fromValue(['a','b'], Type::VARCHAR);

// Map<text,int>
MapCollection::fromValue(['a' => 1], Type::ASCII, Type::INT);

// Tuple<int,text,boolean>
Tuple::fromValue([1, 'x', true], [Type::INT, Type::VARCHAR, Type::BOOLEAN]);

// UDT<id:int, name:text>
UDT::fromValue(['id' => 1, 'name' => 'n'], ['id' => Type::INT, 'name' => Type::VARCHAR]);

// Frozen list<udt<id:int, friends<list<text>>>>
ListCollection::fromValue(
    [
        ['id' => 1, 'friends' => ['a','b']],
        ['id' => 2, 'friends' => []],
    ],
    [
        'type' => Type::LIST,
        'valueType' => [
            'type' => Type::UDT,
            'isFrozen' => true,
            'valueTypes' => [
                'id' => Type::INT,
                'friends' => [
                    'type' => Type::LIST,
                    'valueType' => Type::VARCHAR,
                ],
            ],
        ],
        'isFrozen' => true,
    ]
);

// Map<text, tuple<int, udt<code:int, tags<set<text>>>>>
MapCollection::fromValue(
    [
        'a' => [1, ['code' => 7, 'tags' => ['x','y']]],
    ],
    [
        'type' => Type::MAP,
        'keyType' => Type::VARCHAR,
        'valueType' => [
            'type' => Type::TUPLE,
            'valueTypes' => [
                Type::INT,
                [
                    'type' => Type::UDT,
                    'valueTypes' => [
                        'code' => Type::INT,
                        'tags' => [
                            'type' => Type::SET,
                            'valueType' => Type::VARCHAR,
                        ],
                    ],
                ],
            ],
        ],
    ]
);

// UDT with nested list<map<text, tuple<int,text>>>
UDT::fromValue(
    [
        'id' => 1,
        'items' => [
            ['a' => [1, 'one']],
            ['b' => [2, 'two']],
        ],
    ],
    [
        'id' => Type::INT,
        'items' => [
            'type' => Type::LIST,
            'valueType' => [
                'type' => Type::MAP,
                'keyType' => Type::VARCHAR,
                'valueType' => [
                    'type' => Type::TUPLE,
                    'valueTypes' => [Type::INT, Type::VARCHAR],
                ],
            ],
        ],
    ]
);
```

Nested complex example (Set<UDT> inside a row):
```php
use Cassandra\Value\SetCollection;
use Cassandra\Type;

SetCollection::fromValue([
    [
        'id' => 1,
        'name' => 'string',
        'active' => true,
        'friends' => ['a', 'b'],
        'drinks' => [['qty' => 5, 'brand' => 'Pepsi']],
    ],
], [
    [
        'type' => Type::UDT,
        'definition' => [
            'id' => Type::INT,
            'name' => Type::VARCHAR,
            'active' => Type::BOOLEAN,
            'friends' => ['type' => Type::LIST, 'value' => Type::VARCHAR],
            'drinks' => ['type' => Type::LIST, 'value' => [
                'type' => Type::UDT,
                'typeMap' => ['qty' => Type::INT, 'brand' => Type::VARCHAR],
            ]],
        ],
    ],
]);
```

Special values:
- `new \Cassandra\Value\NotSet()` encodes a bind variable as NOT SET (distinct from NULL)

Events
------

Register a listener and subscribe for events on the connection:
```php
use Cassandra\EventListener;
use Cassandra\Response\Event;
use Cassandra\Request\Register;
use Cassandra\EventType;

$conn->addEventListener(new class () implements EventListener {
    public function onEvent(Event $event): void {
        // Inspect $event->getType() and $event->getData()
    }
});

$conn->syncRequest(new Register([
    EventType::TOPOLOGY_CHANGE,
    EventType::STATUS_CHANGE,
    EventType::SCHEMA_CHANGE,
]));

// process events (simplest possible loop)
while (true) {
    $conn->flush();
    sleep(1);
}
```

Tracing and custom payloads (advanced)
-------------------------------------

You can enable tracing and set a custom payload on any request:
```php
use Cassandra\Request\Query;

$req = new Query('SELECT now() FROM system.local');
$req->enableTracing();
$req->setPayload(['my-key' => 'my-value']);

$result = $conn->syncRequest($req);
```

Asynchronous API
-----------------

The async API lets you pipeline multiple requests without blocking. Each async method returns a `Cassandra\Statement` handle that you can resolve later. You can either block per statement (`getResult()` / `getRowsResult()`), or pump the connection (`flush()`) to advance all in-flight statements.

Basics:
```php
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Consistency;

// Fire two queries concurrently
$s1 = $conn->queryAsync('SELECT count(*) FROM ks.t1', options: new QueryOptions(pageSize: 1000));
$s2 = $conn->queryAsync('SELECT count(*) FROM ks.t2', options: new QueryOptions(pageSize: 1000));

// Do other work here...

// Resolve in any order
$r2 = $s2->getRowsResult();
$r1 = $s1->getRowsResult();
```

Pumping the connection:
```php
// Issue several statements
$handles = [];
for ($i = 0; $i < 10; $i++) {
    $handles[] = $conn->queryAsync('SELECT now() FROM system.local');
}

// Pump until all complete
while (true) {
    $conn->flush();

    $allDone = true;
    foreach ($handles as $h) {
        if (!$h->isResultReady()) { $allDone = false; break; }
    }
    if ($allDone) break;
}

foreach ($handles as $h) {
    $rows = $h->getRowsResult();
    // process
}
```

Prepared + async:
```php
use Cassandra\Request\Options\PrepareOptions;
use Cassandra\Request\Options\ExecuteOptions;

// Prepare asynchronously
$pStmt = $conn->prepareAsync('SELECT id, name FROM ks.users WHERE org_id = ?');
$prepared = $pStmt->getPreparedResult();

// Execute asynchronously with paging
$s = $conn->executeAsync(
    $prepared,
    [123],
    consistency: Consistency::LOCAL_QUORUM,
    options: new ExecuteOptions(pageSize: 200)
);

// Block for rows when you need them
$rows = $s->getRowsResult();
```

Compression
-----------

Enable LZ4 compression (if supported by the server) via `ConnectionOptions`:
```php
use Cassandra\Connection;
use Cassandra\Connection\ConnectionOptions;

$conn = new Connection(
    $nodes,
    keyspace: 'app',
    options: new ConnectionOptions(enableCompression: true)
);
```

Error handling
--------------

- Client-side errors throw `\Cassandra\Exception` (e.g., connection issues, invalid input, unsupported options).
- Server-side errors are raised as subclasses of `\Cassandra\Response\Exception` (e.g., `Unavailable`, `ReadTimeout`, `WriteTimeout`, `Invalid`, `Syntax`, `Unauthorized`, etc.).
- Statement helpers throw `\Cassandra\Exception\StatementException` if the result type doesn’t match the getter you call.

Recommended pattern:
```php
use Cassandra\Consistency;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Response\Exception as ServerException;

try {
    $rows = $conn->query(
        'SELECT id, name FROM ks.users WHERE id = ?',
        [$id],
        Consistency::LOCAL_QUORUM,
        new QueryOptions(pageSize: 500)
    )->asRowsResult();

    foreach ($rows as $row) {
        // ...
    }
} catch (ServerException $e) {
    // Handle server error (Cassandra responded with an error)
} catch (\Cassandra\Connection\Exception $e) {
    // Handle connection/transport error
} catch (\Cassandra\Exception $e) {
    // Handle other client-side errors
}
```

Key features and options
------------------------

- Transports:
  - Sockets: `SocketNodeConfig` (requires PHP sockets ext)
  - Streams: `StreamNodeConfig` (supports SSL/TLS, persistent connections)
- Connection options (`ConnectionOptions`):
  - enableCompression: enable LZ4 if both client and server support it
  - throwOnOverload: request server to throw on overload (protocol v4+)
  - nodeSelectionStrategy: `Random` (default) or `RoundRobin`
  - preparedResultCacheSize: cache size for prepared metadata (default 100)
- Request options:
  - QueryOptions: `autoPrepare` (default true), `pageSize`, `pagingState`, `serialConsistency`, `defaultTimestamp`, `namesForValues` (auto-detected if not set), `keyspace` (v5), `nowInSeconds` (v5)
  - ExecuteOptions: same as QueryOptions plus `skipMetadata`
  - PrepareOptions: `keyspace` (v5)
  - BatchOptions: `serialConsistency`, `defaultTimestamp`, `keyspace` (v5), `nowInSeconds` (v5)
- Protocol v5 conveniences:
  - Per-request `keyspace` and `now_in_seconds` are supported when the server negotiates v5.

Notes
-----

- `pageSize` is clamped to a minimum of 100 by the client for efficiency.
- If you supply non-`Value\*` PHP values with `QueryOptions(autoPrepare: true)`, the driver auto-prepares + executes for correct typing.
- On `UNPREPARED` server errors, the driver transparently re-prepares and retries the execution.
- Always use fully-qualified table names in `PREPARE` statements.

Best practices and tuning
-------------------------

- Retries and idempotency: only retry idempotent reads/writes; avoid retrying non-idempotent mutations.
- Paging: prefer page sizes 100–1000; iterate with `RowsResult` rather than `fetchAll()` for large results.
- Timeouts: set socket/stream timeouts appropriate to workload; use LOCAL_QUORUM for low-latency reads.
- Prepared statements: reuse prepared metadata (`ExecuteOptions::skipMetadata`) across pages.
- Batches: use for atomicity of related partitions; do not batch across many partitions for throughput.
- Keyspace (v5): prefer per-request keyspace for multi-tenant use; otherwise set once at connect.

Connection tuning examples
--------------------------

```php
use Cassandra\Connection;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\Connection\ConnectionOptions;

// Socket with custom timeouts
$socket = new SocketNodeConfig(
    host: '127.0.0.1',
    port: 9042,
    username: 'user',
    password: 'secret',
    socketOptions: [
        SO_RCVTIMEO => ['sec' => 5, 'usec' => 0],
        SO_SNDTIMEO => ['sec' => 5, 'usec' => 0],
    ]
);

// Stream with TLS and persistent
$stream = new StreamNodeConfig(
    host: 'tls://cassandra.example.com',
    port: 9042,
    username: 'user',
    password: 'secret',
    connectTimeoutInSeconds: 5,
    timeoutInSeconds: 15,
    persistent: true,
    sslOptions: [
        'cafile' => '/etc/ssl/certs/ca.pem',
        'verify_peer' => true,
        'verify_peer_name' => true,
    ]
);

$conn = new Connection([$socket, $stream], options: new ConnectionOptions(enableCompression: true));
```

Configuring value encoding
--------------------------

```php
use Cassandra\Connection;
use Cassandra\Value\ValueEncodeConfig;
use Cassandra\Value\EncodeOption\TimestampEncodeOption;
use Cassandra\Value\EncodeOption\DateEncodeOption;

$conn = new Connection([$socket]);
$conn->configureValueEncoding(new ValueEncodeConfig(
    timestampEncodeOption: TimestampEncodeOption::AS_INT,
    dateEncodeOption: DateEncodeOption::AS_INT,
));
```

Warnings listener
-----------------

```php
use Cassandra\WarningsListener;
use Cassandra\Request\Request;
use Cassandra\Response\Response;

$conn->registerWarningsListener(new class () implements WarningsListener {
    public function onWarnings(array $warnings, Request $request, Response $response): void {
        error_log('Cassandra warnings: ' . implode('; ', $warnings));
    }
});
```

Event processing patterns
-------------------------

```php
use Cassandra\EventListener;
use Cassandra\Response\Event;

$conn->addEventListener(new class () implements EventListener {
    public function onEvent(Event $event): void {
        // enqueue to worker, react to topology/status/schema changes
    }
});

// Non-busy loop with backoff
while (true) {
    $conn->flush();
    usleep(200_000); // 200ms
}
```

v5 keyspace per request
-----------------------

- When the server negotiates protocol v5, you can set `keyspace` on `QueryOptions`, `ExecuteOptions`, and `PrepareOptions`.
- If you also call `setKeyspace()`, the per-request option takes precedence for that request.

Batch guidance
--------------

- Logged batch: atomic across partitions; higher coordination cost.
- Unlogged batch: for same-partition groups; lower overhead.
- Counter batch: only for counter tables.
- Keep batches small (dozens of statements, not thousands).

Tracing notes
-------------

- Use tracing sparingly in production; it adds overhead.
- Read the trace id from the result to correlate with server logs (if enabled).

Performance tips
----------------

- Prefer prepared statements for hot paths; the driver caches prepared metadata.
- Iterate results instead of materializing large arrays.
- Avoid large `IN (...)` sets; model for efficient partition reads.

Troubleshooting
---------------

- Protocol mismatch: ensure server supports v3/v4/v5; the driver negotiates automatically. Check `getVersion()`.
- Authentication failures: set `username`/`password` on `NodeConfig`.
- Compression errors: only enable LZ4 when the server advertises it in `OPTIONS`.
- Timeout/transport errors: adjust socket/stream timeouts; verify network and DC latency.

Version support
---------------

- Cassandra 4.x/5.x tested. Protocols v3/v4/v5 supported; features like per-request keyspace/now_in_seconds require v5.

Security
--------

- Validate TLS certificates (`verify_peer`, `verify_peer_name`).
- Avoid embedding credentials in code; use environment variables or secrets management.
- Do not disable TLS verification in production.

Type mapping quick reference
----------------------------

```php
// PHP -> Cassandra examples
use Cassandra\Value\Uuid;
use Cassandra\Value\Varchar;
use Cassandra\Value\Int32;

Uuid::fromValue('...');       // uuid
Varchar::fromValue('text');   // text/varchar
Int32::fromValue(123);        // int
```

Error-handling matrix
---------------------

- Client connection/transport issues: throw `Cassandra\Connection\Exception`
  - Examples: DNS/socket failures, timeouts, protocol mismatch
  - Action: verify node reachability, credentials, protocol support, compression settings
- Server-side errors: throw subclasses of `Cassandra\Response\Exception`
  - Examples: `Unavailable`, `ReadTimeout`, `WriteTimeout`, `Overloaded`, `Truncate`, `Syntax`, `Invalid`, `Unauthorized`
  - Action: catch as `Cassandra\Response\Exception` (or specific subclass) and handle per your retry/policy
- Statement result mismatches: throw `Cassandra\Exception\StatementException`
  - Examples: calling `getRowsResult()` on a non-rows result
  - Action: adjust query or use correct result accessor

Type mapping reference
----------------------

```php
// Common Cassandra <-> PHP mappings (constructor via ::fromValue)
use Cassandra\Type;
use Cassandra\Value\Ascii;      // ascii
use Cassandra\Value\Varchar;    // text/varchar
use Cassandra\Value\Blob;       // blob (binary string)
use Cassandra\Value\Boolean;    // boolean
use Cassandra\Value\Int32;      // int
use Cassandra\Value\Bigint;     // bigint
use Cassandra\Value\Varint;     // varint
use Cassandra\Value\Smallint;   // smallint
use Cassandra\Value\Tinyint;    // tinyint
use Cassandra\Value\Float32;    // float
use Cassandra\Value\Double;     // double
use Cassandra\Value\Inet;       // inet
use Cassandra\Value\Uuid;       // uuid
use Cassandra\Value\Timeuuid;   // timeuuid
use Cassandra\Value\Date;       // date
use Cassandra\Value\Time;       // time
use Cassandra\Value\Timestamp;  // timestamp
use Cassandra\Value\Duration;   // duration
use Cassandra\Value\ListCollection; // list
use Cassandra\Value\SetCollection;  // set
use Cassandra\Value\MapCollection;  // map
use Cassandra\Value\Tuple;          // tuple
use Cassandra\Value\UDT;            // user-defined type
use Cassandra\Value\Vector;         // vector

Ascii::fromValue('a');
Varchar::fromValue('hello');
Blob::fromValue("\x01\x02");
Boolean::fromValue(true);
Int32::fromValue(1);
Bigint::fromValue(10_000_000_000);
Varint::fromValue('12345678901234567890');
Smallint::fromValue(1000);
Tinyint::fromValue(12);
Float32::fromValue(1.23);
Double::fromValue(2.34);
Inet::fromValue('192.168.0.1');
Uuid::fromValue('00000000-0000-0000-0000-000000000000');
Timeuuid::fromValue('f47ac10b-58cc-11cf-a447-00100d4b9e00');
Date::fromValue('2011-02-03');
Time::fromValue('08:12:54.123456789');
Timestamp::fromValue('2011-02-03T04:05:00.000+0000');
Duration::fromValue('1d2h3m');
ListCollection::fromValue([1,2,3], [Type::INT]);
SetCollection::fromValue([1,2,3], [Type::INT]);
MapCollection::fromValue(['a' => 1], Type::ASCII, Type::INT);
Tuple::fromValue([1, 'x'], [Type::INT, Type::VARCHAR]);
UDT::fromValue(['id' => 1, 'name' => 'n'], ['id' => Type::INT, 'name' => Type::VARCHAR]);
Vector::fromValue([0.1, 0.2, -0.3]);
```

API reference (essentials)
-------------------------

- `Cassandra\Connection`
  - `connect()`, `disconnect()`, `isConnected()`, `getVersion()`
  - `setConsistency(Consistency)`, `withConsistency(Consistency)`
  - `setKeyspace(string)`, `withKeyspace(string)`, `supportsKeyspaceRequestOption()`, `supportsNowInSecondsRequestOption()`
  - `query(string, array = [], ?Consistency, QueryOptions)` / `queryAsync(...)` / `queryAll(...)`
  - `prepare(string, PrepareOptions)` / `prepareAsync(...)`
  - `execute(Result $previous, array = [], ?Consistency, ExecuteOptions)` / `executeAsync(...)` / `executeAll(...)`
  - `batch(Batch)` / `batchAsync(Batch)`
  - `syncRequest(Request)` / `asyncRequest(Request)` / `flush()`
  - `addEventListener(EventListener)`

- Results
  - `RowsResult` (iterable): `fetch()`, `fetchAll()`, `fetchColumn()`, `fetchAllColumns()`, `fetchKeyPair()`, `fetchAllKeyPairs()`, `configureFetchObject()`, `fetchObject()`, `fetchAllObjects()`, `getRowsMetadata()`, `hasMorePages()`
  - `PreparedResult` (for execute)
  - `SchemaChangeResult`, `SetKeyspaceResult`, `VoidResult`

- Types
  - `Cassandra\Consistency` (enum)
  - `Cassandra\SerialConsistency` (enum)
  - `Cassandra\Type` (enum) and `Cassandra\Value\*` classes (Ascii, Bigint, Blob, Boolean, Counter, Date, Decimal, Double, Duration, Float32, Inet, Int32, ListCollection, MapCollection, NotSet, SetCollection, Smallint, Time, Timestamp, Timeuuid, Tinyint, Tuple, UDT, Uuid, Varchar, Varint, Vector, ...)

Running tests
-------------

- Unit tests:

```bash
composer install
composer test:unit
```

- Integration tests (Dockerized Cassandra 5):

```bash
composer test:integration
```

You can manage steps manually:

```bash
composer test:integration:up
composer test:integration:init
composer test:integration:run
composer test:integration:down
```

License
-------

MIT

Credits
-------

Inspired by and building upon work from:
- duoshuo/php-cassandra
- arnaud-lb/php-cassandra

