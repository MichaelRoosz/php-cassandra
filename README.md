php-cassandra: A modern Cassandra client for PHP
================================================

php-cassandra is a pure-PHP client for Apache Cassandra with support for CQL binary protocol v3, v4 and v5 (Cassandra 4.x/5.x), synchronous and asynchronous APIs, prepared statements, batches, result iterators, object mapping, SSL/TLS, and LZ4 compression.

Package: https://packagist.org/packages/mroosz/php-cassandra

### Highlights
- Protocol v3/v4/v5, auto-negotiated
- Two transports: sockets and PHP streams (streams support SSL/TLS and persistent connections)
- Synchronous and asynchronous requests
- Prepared statements with named or positional binding
- Batches: logged, unlogged, counter
- Full data type coverage (collections, tuples, UDTs, custom)
- Iterators plus multiple fetch styles (ASSOC, NUM, BOTH) and object mapping
- Events (schema/status/topology) with a simple listener interface
- Optional LZ4 compression and server overload signalling

### Requirements
- PHP 8.1+
- 64-bit PHP for 64-bit types like Bigint, Counter, Duration, Time, Timestamp
- For socket transport: PHP sockets extension; stream transport has no extra extension requirements

## Installation

Using Composer:
```bash
composer require mroosz/php-cassandra
```

Or load the library without Composer:
```php
require __DIR__ . '/php-cassandra/php-cassandra.php';
```

## Running tests

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

## Quick start

```php
<?php

use Cassandra\Connection;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\Consistency;

// Choose one or more nodes and a transport
$nodes = [
    new SocketNodeConfig(host: '127.0.0.1', port: 9042, username: 'cassandra', password: 'cassandra'),
    // or streams (supports SSL/TLS and persistent connections)
    // new StreamNodeConfig(host: '127.0.0.1', port: 9042, username: 'cassandra', password: 'cassandra'),
];

// Optional connection options (protocol STARTUP options)
$options = [
    // 'COMPRESSION' => 'lz4',
    // 'THROW_ON_OVERLOAD' => '1', // protocol v4+
];

$conn = new Connection($nodes, keyspace: 'my_keyspace', options: $options);
$conn->connect();

// Consistency default for subsequent requests
$conn->setConsistency(Consistency::QUORUM);

// Plain query (positional bind)
$rows = $conn->querySync(
    'SELECT * FROM users WHERE id = ?',
    [new \Cassandra\Type\Uuid('c5420d81-499e-4c9c-ac0c-fa6ba3ebc2bc')]
)->fetchAll();

// Prepared statement (named bind + paging)
$prepared = $conn->prepareSync('SELECT id, name FROM users WHERE org_id = :org_id');

$result = $conn->executeSync(
    $prepared,
    values: ['org_id' => 42],
    consistency: Consistency::LOCAL_QUORUM,
    options: new \Cassandra\Request\Options\ExecuteOptions(
        pageSize: 100,
        namesForValues: true
    )
);

foreach ($result as $row) {
    echo $row['name'], "\n";
}
```

## Connecting

Create `NodeConfig` instances and pass them to `Connection`:

```php
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\Connection;

$socketNode =new SocketNodeConfig(host: '10.0.0.10', port: 9042, username: 'user', password: 'secret',
        socketOptions: [SO_RCVTIMEO => ['sec' => 10, 'usec' => 0]]);

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

$conn = new Connection([$socketNode, $tlsNode], keyspace: 'app');
$conn->connect();
```

Startup options (third constructor argument) support:
- `COMPRESSION` = `lz4` if enabled on server
- `THROW_ON_OVERLOAD` = `'1'` or `'0'` (v4+)

Keyspace selection:
- v5: can also be sent per-request via Query/Execute options (see below)
- v3/v4: call `$conn->setKeyspace('ks')` or run `USE ks`

## Consistency levels

Use the `Consistency` enum:
- `ALL`, `ANY`, `EACH_QUORUM`, `LOCAL_ONE`, `LOCAL_QUORUM`, `LOCAL_SERIAL`, `ONE`, `QUORUM`, `SERIAL`, `THREE`, `TWO`

Apply per call or as default via `setConsistency()`.

## Queries

Synchronous:
```php
$rowsResult = $conn->querySync(
    'SELECT id, name FROM users WHERE id = ?',
    [new \Cassandra\Type\Uuid($id)],
    consistency: \Cassandra\Consistency::ONE,
    options: new \Cassandra\Request\Options\QueryOptions(pageSize: 100)
);
```

Asynchronous:
```php
$s1 = $conn->queryAsync('SELECT count(*) FROM t1');
$s2 = $conn->queryAsync('SELECT count(*) FROM t2');

$r2 = $s2->getResult();
$r1 = $s1->getResult();
```

Query options (`QueryOptions`):
- `pageSize` (int)
- `pagingState` (string)
- `serialConsistency` (`SerialConsistency::SERIAL` or `SerialConsistency::LOCAL_SERIAL`)
- `defaultTimestamp` (ms since epoch)
- `namesForValues` (bool): true to use associative binds
- `keyspace` (string; protocol v5 only)
- `nowInSeconds` (int; protocol v5 only)

## Prepared statements

```php
$prepared = $conn->prepareSync('SELECT * FROM users WHERE email = :email');

$rowsResult = $conn->executeSync(
    $prepared,
    ['email' => 'jane@example.com'],
    options: new \Cassandra\Request\Options\ExecuteOptions(
        namesForValues: true,
        pageSize: 50
    )
);
```

Pagination with prepared statements:
```php
$options = new \Cassandra\Request\Options\ExecuteOptions(pageSize: 100, namesForValues: true);
$result = $conn->executeSync($prepared, ['org_id' => 1], options: $options);

do {
    foreach ($result as $row) {
        // process row
    }

    $pagingState = $result->getRowsMetadata()->pagingState;
    if ($pagingState === null) break;

    $options = new \Cassandra\Request\Options\ExecuteOptions(
        pageSize: 100,
        namesForValues: true,
        pagingState: $pagingState
    );
    $result = $conn->executeSync($result, [], options: $options); // reuse previous RowsResult for metadata id
} while (true);
```

## Batches

```php
use Cassandra\Request\Batch;
use Cassandra\Request\BatchType;

$batch = new Batch(type: BatchType::LOGGED, consistency: Consistency::QUORUM);

// Prepared in batch
$prepared = $conn->prepareSync('UPDATE users SET age = :age WHERE id = :id');
$batch->appendPreparedStatement($prepared, ['age' => 21, 'id' => 'c5419d81-499e-4c9c-ac0c-fa6ba3ebc2bc']);

// Simple query in batch
$batch->appendQuery(
    'INSERT INTO users (id, name, age) VALUES (?, ?, ?)',
    [
        new \Cassandra\Type\Uuid('c5420d81-499e-4c9c-ac0c-fa6ba3ebc2bc'),
        new \Cassandra\Type\Varchar('Mark'),
        20,
    ]
);

$conn->batchSync($batch);
```

Batch options (`BatchOptions`): `serialConsistency`, `defaultTimestamp`, `keyspace` (v5), `nowInSeconds` (v5).

## Results and fetching

`querySync()`/`executeSync()` return a `RowsResult` for row-returning queries. Supported methods:
- `fetch(FetchType::ASSOC|NUM|BOTH)` returns next row or false
- `fetchAll(FetchType)` returns all remaining rows
- `fetchColumn(int $index)`/`fetchAllColumns(int $index)`
- `fetchKeyPair(int $keyIndex, int $valueIndex)`/`fetchAllKeyPairs(...)`
- `getIterator()` returns a `ResultIterator` so you can `foreach ($rowsResult as $row)`

Example:
```php
use Cassandra\Response\Result\FetchType;

$r = $conn->querySync('SELECT role FROM system_auth.roles');
foreach ($r as $i => $row) {
    echo $row['role'], "\n";
}

$names = $r->fetchAllColumns(0); // remaining rows of first column
```

### Object mapping

You can fetch rows into objects by implementing `RowClassInterface` or by using the default `RowClass`:

```php
final class UserRow implements \Cassandra\Response\Result\RowClassInterface {
    public function __construct(private array $row, array $args = []) {}
    public function id(): string { return (string) $this->row['id']; }
    public function name(): string { return (string) $this->row['name']; }
}

$rows = $conn->querySync('SELECT id, name FROM users');
$rows->configureFetchObject(UserRow::class);

foreach ($rows as $user) {
    echo $user->name(), "\n";
}
```

## Data types

All native Cassandra types are supported via classes in `Cassandra\Type\*`. You may pass either:
- A concrete `Type\...` instance, or
- A PHP scalar/array matching the type; the driver will convert it when metadata is available

Examples:
```php
// Scalars
new \Cassandra\Type\Ascii('hello');
new \Cassandra\Type\Bigint(10_000_000_000);
new \Cassandra\Type\Boolean(true);
new \Cassandra\Type\Double(2.718281828459);
new \Cassandra\Type\Float32(2.718);
new \Cassandra\Type\Integer(123);
new \Cassandra\Type\Smallint(2048);
new \Cassandra\Type\Tinyint(12);
new \Cassandra\Type\Varint(10000000000);

// Temporal
\Cassandra\Type\Date::fromString('2011-02-03');
\Cassandra\Type\Time::fromString('08:12:54.123456789');
\Cassandra\Type\Timestamp::fromString('2011-02-03T04:05:00.000+0000');
\Cassandra\Type\Duration::fromString('89h4m48s');

// Collections / Tuples / UDT
new \Cassandra\Type\ListCollection([1, 2, 3], [\Cassandra\Type::INT]);
new \Cassandra\Type\SetCollection([1, 2, 3], [\Cassandra\Type::INT]);
new \Cassandra\Type\MapCollection(['a' => 1], [\Cassandra\Type::ASCII, \Cassandra\Type::INT]);
new \Cassandra\Type\Tuple([1, 'x'], [\Cassandra\Type::INT, \Cassandra\Type::VARCHAR]);
new \Cassandra\Type\UDT(['id' => 1, 'name' => 'n'], ['id' => \Cassandra\Type::INT, 'name' => \Cassandra\Type::VARCHAR]);
```

Nested complex example (Set<UDT> inside a row):
```php
new \Cassandra\Type\SetCollection([
    [
        'id' => 1,
        'name' => 'string',
        'active' => true,
        'friends' => ['a', 'b'],
        'drinks' => [['qty' => 5, 'brand' => 'Pepsi']],
    ],
], [
    [
        'type' => \Cassandra\Type::UDT,
        'definition' => [
            'id' => \Cassandra\Type::INT,
            'name' => \Cassandra\Type::VARCHAR,
            'active' => \Cassandra\Type::BOOLEAN,
            'friends' => ['type' => \Cassandra\Type::LIST_COLLECTION, 'value' => \Cassandra\Type::VARCHAR],
            'drinks' => ['type' => \Cassandra\Type::LIST_COLLECTION, 'value' => [
                'type' => \Cassandra\Type::UDT,
                'typeMap' => ['qty' => \Cassandra\Type::INT, 'brand' => \Cassandra\Type::VARCHAR],
            ]],
        ],
    ],
]);
```

Special values:
- `new \Cassandra\Type\NotSet()` encodes a bind variable as NOT SET, not resulting in any change to the existing value. (distinct from NULL)

## Events

Register a listener and subscribe for events on the connection:
```php
$conn->addEventListener(new class () implements \Cassandra\EventListener {
    public function onEvent(\Cassandra\Response\Event $event): void {
        // inspect $event->getType() and $event->getData()
    }
});

use Cassandra\Request\Register;
use Cassandra\Response\EventType;

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

## Tracing and custom payloads (advanced)

You can enable tracing and set a custom payload on any request:
```php
use Cassandra\Request\Query;

$req = new Query('SELECT now() FROM system.local');
$req->enableTracing();
$req->setPayload(['my-key' => 'my-value']);

$result = $conn->syncRequest($req);
```

## Compression

Enable LZ4 compression if supported by the server by passing the startup option:
```php
$conn = new Cassandra\Connection($nodes, options: ['COMPRESSION' => 'lz4']);
```

## Error handling

All operations throw `\Cassandra\Exception` for client errors and `\Cassandra\Response\Exception` for server-side errors (e.g., invalid query, unavailable, timeouts). Prepared statements are transparently re-prepared when needed.

## API reference (essentials)

- `Cassandra\Connection`
  - `connect()`, `disconnect()`
  - `setConsistency(Consistency)`
  - `querySync(string, array = [], ?Consistency, QueryOptions)` / `queryAsync(...)`
  - `prepareSync(string, PrepareOptions)` / `prepareAsync(...)`
  - `executeSync(PreparedResult|RowsResult, array = [], ?Consistency, ExecuteOptions)` / `executeAsync(...)`
  - `batchSync(Batch)` / `batchAsync(Batch)`
  - `syncRequest(Request)` / `asyncRequest(Request)`
  - `addEventListener(EventListener)`

- `Cassandra\Request\Options\QueryOptions | ExecuteOptions | BatchOptions`
- `Cassandra\Request\Batch`, `BatchType`
- `Cassandra\Response\Result\RowsResult` (iterable, fetch helpers)
- `Cassandra\Response\Result\RowClassInterface`, `RowClass`
- `Cassandra\Consistency` (enum)
- `Cassandra\Type` (enum) and `Cassandra\Type\*` classes

## License

MIT

## Credits

Inspired by and building upon work from:
- duoshuo/php-cassandra
- arnaud-lb/php-cassandra
