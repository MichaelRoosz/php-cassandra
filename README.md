php-cassandra
=============

[![Latest Stable Version](https://poser.pugx.org/mroosz/php-cassandra/v/stable)](https://packagist.org/packages/mroosz/php-cassandra)
[![License](https://poser.pugx.org/mroosz/php-cassandra/license)](https://packagist.org/packages/mroosz/php-cassandra)
[![PHP Version Require](https://poser.pugx.org/mroosz/php-cassandra/require/php)](https://packagist.org/packages/mroosz/php-cassandra)
[![Total Downloads](https://poser.pugx.org/mroosz/php-cassandra/downloads)](https://packagist.org/packages/mroosz/php-cassandra)

A pure-PHP client for Apache Cassandra and ScyllaDB. No extension required.

- CQL binary protocol v3, v4 and v5 with automatic negotiation
- Synchronous and asynchronous requests, prepared statements, batches and paging
- Every CQL type, including collections, tuples, UDTs and vectors
- TLS, password and pluggable authentication, LZ4 compression
- Request timeouts and a heartbeat that detects dead connections

Upgrading from 1.x? See the [upgrade notes](UPGRADE.md).

Contents
--------

- [Requirements](#requirements) · [Installation](#installation) · [Quick start](#quick-start)
- [Connecting](#connecting) · [Queries](#queries) · [Results](#results) · [Batches](#batches)
- [Data types](#data-types) · [Maps](#maps) · [Reading values](#reading-values)
- [Collection updates](#collection-updates) · [Lightweight transactions](#lightweight-transactions) · [JSON](#json)
- [Async requests](#async-requests) · [Timeouts](#timeouts) · [Events and warnings](#events-and-warnings)
- [Errors](#errors) · [Compression](#compression) · [Tracing and custom payloads](#tracing-and-custom-payloads)
- [Server compatibility](#server-compatibility) · [Migrating from the DataStax driver](#migrating-from-the-datastax-driver)
- [Benchmarks](#benchmarks) · [API overview](#api-overview) · [Contributing](#contributing)

Requirements
------------

PHP 8.1 or newer. These extensions are optional:

| Extension | Purpose |
|---|---|
| `openssl` | TLS connections |
| `sockets` | `SocketNodeConfig`; `StreamNodeConfig` needs no extension |
| `lz4` | Much faster LZ4; a pure-PHP implementation is the fallback |
| `gmp` or `bcmath` | Faster `varint` and `decimal`; a pure-PHP calculator is the fallback |

On 32-bit PHP, `date`, `duration`, `time`, `timestamp` and the `defaultTimestamp` option are unavailable, and `bigint` and `counter` work only within the 32-bit range.

Installation
------------

```bash
composer require mroosz/php-cassandra
```

Without Composer, `require` the bundled `php-cassandra.php` autoloader instead.

Quick start
-----------

```php
use Cassandra\Connection;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\Consistency;
use Cassandra\Exception\CassandraException;

$conn = new Connection(
    [new StreamNodeConfig(host: '127.0.0.1', port: 9042, username: 'cassandra', password: 'cassandra')],
    keyspace: 'my_keyspace',
);
$conn->connect();
$conn->setConsistency(Consistency::LOCAL_QUORUM);

try {
    // A query with values is prepared automatically so that they are typed correctly.
    $conn->query('INSERT INTO users (id, name) VALUES (?, ?)', [42, 'Ada']);

    $rows = $conn->query('SELECT id, name FROM users WHERE id = ?', [42])->asRowsResult();
    foreach ($rows as $row) {
        echo $row['name'], "\n";
    }
} catch (CassandraException $e) {
    // The library only throws exceptions from Cassandra\Exception.
    error_log($e->getMessage());
}
```

Connecting
----------

### Transports

| | `StreamNodeConfig` | `SocketNodeConfig` |
|---|---|---|
| Requires | nothing | `ext-sockets` |
| TLS | yes | no |
| Transport timeouts | `timeoutInSeconds` (default 15) | `SO_RCVTIMEO` (default 15s), `SO_SNDTIMEO` (default 10s) |
| Connect timeout | `connectTimeoutInSeconds` (default 5) | `connectTimeoutInSeconds` (default 5) |

The host can be a name or an IPv4 or IPv6 address. A name resolving to several addresses is tried address by address. With several nodes, the next is tried when one fails, including during the handshake.

```php
use Cassandra\Connection;
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Connection\NodeSelectionStrategy;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Connection\StreamNodeConfig;

$conn = new Connection(
    [
        new StreamNodeConfig(host: '10.0.0.1', timeoutInSeconds: 15),
        new SocketNodeConfig(host: '10.0.0.2', socketOptions: [SO_KEEPALIVE => 1]),
    ],
    keyspace: 'app',
    options: new ConnectionOptions(
        enableCompression: true,            // default false
        nodeSelectionStrategy: NodeSelectionStrategy::RoundRobin, // default Random
        preparedResultCacheSize: 200,       // default 100
        requestTimeoutInSeconds: 30,        // default 30, see Timeouts
        heartbeatIntervalInSeconds: 30,     // default 30, null disables it
        heartbeatTimeoutInSeconds: 5,       // default 5
        maxOrphanedStreams: 24,             // default 24
        throwOnOverload: true,              // default false, v4+
    ),
);
```

### TLS

A non-empty `sslOptions` enables TLS and takes any [PHP SSL context option](https://www.php.net/manual/en/context.ssl.php). PHP verifies the peer by default.

```php
use Cassandra\Connection\StreamNodeConfig;

$node = new StreamNodeConfig(
    host: 'cassandra.example.com',
    username: 'user',
    password: 'secret',
    sslOptions: [
        'cafile' => '/etc/ssl/certs/ca.pem',
        'local_cert' => '/path/to/client.pem', // for mutual TLS
        'local_pk' => '/path/to/client.key',
        'passphrase' => 'key-passphrase',
    ],
);
```

### Authentication

`username` and `password` cover Cassandra's and ScyllaDB's `PasswordAuthenticator`. For anything else, implement `Authenticator`, which answers the node's SASL exchange and takes precedence over a username and password:

```php
use Cassandra\Connection\Authenticator;
use Cassandra\Connection\StreamNodeConfig;

final class TokenAuthenticator implements Authenticator {
    public function __construct(private string $token) {
    }

    public function initialResponse(string $serverAuthenticator): string {
        return $this->token;
    }

    public function evaluateChallenge(?string $challenge): string {
        return $this->token;
    }

    public function authenticationSucceeded(?string $token): void {
        // Verify the node's final token here if the mechanism is mutual.
    }
}

$node = new StreamNodeConfig(host: '10.0.0.1', authenticator: new TokenAuthenticator('secret'));
```

One instance serves all connection attempts of a node, so a stateful mechanism must reset in `initialResponse()`. Report failures with a `ConnectionException`.

### Keyspace

Set the keyspace in the constructor or with `setKeyspace()`. The name is taken exactly as given: a keyspace created unquoted as `MyKs` is `myks`.

| | Protocol v3/v4 | Protocol v5 |
|---|---|---|
| How it applies | a `USE` on the session | sent with each request |
| A keyspace that does not exist | fails `setKeyspace()` | fails the next request |
| Prepared-statement cache on switching | emptied | kept per keyspace |
| Per-request `keyspace` option | refused | takes precedence |
| Leaving a keyspace with `setKeyspace('')` | refused while connected; `disconnect()` first | allowed |

Prefer fully-qualified table names in prepared statements, so that a statement never depends on the session's keyspace.

Queries
-------

```php
use Cassandra\Consistency;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Request\Options\QueryOptions;

// A one-off query.
$rows = $conn->query(
    'SELECT id, name FROM ks.users WHERE org_id = ?',
    [123],
    Consistency::ONE,
    new QueryOptions(pageSize: 100),
)->asRowsResult();

// A prepared statement, for statements you run often.
$prepared = $conn->prepare('SELECT id, name FROM ks.users WHERE org_id = :org_id');
$rows = $conn->execute($prepared, ['org_id' => 123], options: new ExecuteOptions(pageSize: 100))->asRowsResult();
```

`query()` prepares a statement automatically when it has values that are plain PHP types, and caches it. If the server later forgets a prepared statement, it is prepared again and the request re-sent, for batches too.

Bind values positionally with `?` and a list, or by name with `:name` and an associative array. Quoted numeric names such as `:"0"` need an explicit `namesForValues`, since PHP turns their keys into integers.

### Options

`QueryOptions` and `ExecuteOptions`:

| Option | Meaning |
|---|---|
| `pageSize` | Rows per page; `null` is the server default |
| `pagingState` | Continue from an earlier page |
| `serialConsistency` | `SerialConsistency::SERIAL` or `LOCAL_SERIAL`, for LWT |
| `defaultTimestamp` | Write timestamp in microseconds since the epoch; 64-bit PHP only |
| `namesForValues` | Named values; detected from the array keys by default |
| `keyspace` | Per-request keyspace; v5 only |
| `nowInSeconds` | Server time override; v5 only |
| `requestTimeoutInSeconds` | See [Timeouts](#timeouts) |
| `autoPrepare` | `QueryOptions` only, default `true`: prepare when needed |
| `skipMetadata` | `ExecuteOptions` only: omit result metadata |

`BatchOptions` has `serialConsistency`, `defaultTimestamp`, `keyspace`, `nowInSeconds` and `requestTimeoutInSeconds`. `PrepareOptions` has `keyspace` and `requestTimeoutInSeconds`.

### Paging

`queryAll()` and `executeAll()` fetch every page and return a list of `RowsResult`:

```php
use Cassandra\Request\Options\QueryOptions;

foreach ($conn->queryAll('SELECT * FROM ks.events', options: new QueryOptions(pageSize: 1000)) as $page) {
    foreach ($page as $row) {
        // ...
    }
}
```

To stay in control of memory, fetch one page at a time:

```php
use Cassandra\Request\Options\ExecuteOptions;

$prepared = $conn->prepare('SELECT * FROM ks.events WHERE day = ?');
$rows = $conn->execute($prepared, ['2026-01-01'], options: new ExecuteOptions(pageSize: 1000))->asRowsResult();

while (true) {
    foreach ($rows as $row) {
        // ...
    }

    $pagingState = $rows->getRowsMetadata()->pagingState;
    if ($pagingState === null) {
        break;
    }

    // Pass the page just read, so that its metadata carries over.
    $rows = $conn->execute(
        $rows,
        ['2026-01-01'],
        options: new ExecuteOptions(pageSize: 1000, pagingState: $pagingState),
    )->asRowsResult();
}
```

Results
-------

`query()` and `execute()` return a `Result`. For row-returning statements call `asRowsResult()`, which is iterable:

| Method | Returns |
|---|---|
| `fetch(FetchType)` | The next row, or `false` |
| `fetchAll(FetchType)` | All remaining rows |
| `fetchColumn(int)` / `fetchAllColumns(int)` | One column |
| `fetchKeyPair(int, int)` / `fetchAllKeyPairs(int, int)` | Key and value from two columns |
| `getRowCount()` | Rows in this page |
| `hasMorePages()` | Whether another page exists |

`FetchType` is `ASSOC` (the default), `NUM` or `BOTH`.

### Object mapping

```php
use Cassandra\Response\Result\RowClassInterface;

final class UserRow implements RowClassInterface {
    public function __construct(private array $row, array $args = []) {
    }

    public function name(): string {
        return (string) $this->row['name'];
    }
}

$rows = $conn->query('SELECT id, name FROM ks.users')->asRowsResult();
$rows->configureFetchObject(UserRow::class);

foreach ($rows as $user) {
    echo $user->name(), "\n";
}
```

The default `RowClass` gives property access to columns: `$row->name`.

Batches
-------

```php
use Cassandra\Consistency;
use Cassandra\Request\BatchType;
use Cassandra\Value\Int32;
use Cassandra\Value\Varchar;

$batch = $conn->createBatchRequest(BatchType::LOGGED, Consistency::QUORUM);

// Prepared entries take plain PHP values, positional or named.
$update = $conn->prepare('UPDATE ks.users SET age = :age WHERE id = :id');
$batch->appendPreparedStatement($update, ['age' => 21, 'id' => 1]);

// Simple entries are not prepared, so use value objects where a bare PHP value is ambiguous.
$batch->appendQuery('INSERT INTO ks.users (id, name) VALUES (?, ?)', [Int32::fromValue(2), Varchar::fromValue('Mark')]);

$conn->batch($batch);
```

Data types
----------

With type metadata, which prepared statements and the default `autoPrepare` provide, bind plain PHP values. Without it, as in `autoPrepare: false` and `Batch::appendQuery()`, use the value classes from `Cassandra\Value`: a bare PHP `int` is sent as a 32-bit `int`, and a bare `DateTime` raises an exception rather than guessing.

| CQL | Value class | Example input |
|---|---|---|
| `ascii`, `text`, `varchar` | `Ascii`, `Varchar` | `'hello'` |
| `tinyint`, `smallint`, `int`, `bigint`, `counter` | `Tinyint`, `Smallint`, `Int32`, `Bigint`, `Counter` | `123` |
| `varint`, `decimal` | `Varint`, `Decimal` | `'123456789012345678901'`, `'123.456'` |
| `float`, `double` | `Float32`, `Double` | `2.5` |
| `boolean`, `blob` | `Boolean`, `Blob` | `true`, `"\x01\x02"` |
| `uuid`, `timeuuid` | `Uuid`, `Timeuuid` | canonical string, 32 hex digits or 16 raw bytes |
| `inet` | `Inet` | `'192.168.0.1'`, `'::1'` |
| `date` | `Date` | `'2011-02-03'` |
| `time` | `Time` | `'08:12:54.123456789'` |
| `timestamp` | `Timestamp` | `'2011-02-03T04:05:00+00:00'`; without an offset, UTC |
| `duration` | `Duration` | `'89h4m48s'` |

Use `new Cassandra\Value\NotSet()` to leave a bound value unset, which differs from `null`; this requires v4 or newer.

### Complex types

Value classes for complex types take a type definition. Pass a `Type` for a simple element, or an array for a nested one:

```php
use Cassandra\Type;
use Cassandra\Value\ListCollection;
use Cassandra\Value\MapCollection;
use Cassandra\Value\SetCollection;
use Cassandra\Value\Tuple;
use Cassandra\Value\UDT;
use Cassandra\Value\Vector;

ListCollection::fromValue([1, 2, 3], Type::INT);
SetCollection::fromValue(['a', 'b'], Type::VARCHAR);
MapCollection::fromValue(['a' => 1], Type::VARCHAR, Type::INT);
Tuple::fromValue([1, 'x'], [Type::INT, Type::VARCHAR]);
UDT::fromValue(['id' => 1, 'name' => 'n'], ['id' => Type::INT, 'name' => Type::VARCHAR]);
Vector::fromValue([0.12, -0.3, 0.9], Type::FLOAT, dimensions: 3);

// frozen<list<frozen<udt<id: int, friends: list<text>>>>>
ListCollection::fromValue(
    [['id' => 1, 'friends' => ['a', 'b']]],
    [
        'type' => Type::UDT,
        'isFrozen' => true,
        'valueTypes' => [
            'id' => Type::INT,
            'friends' => ['type' => Type::LIST, 'valueType' => Type::VARCHAR],
        ],
    ],
    isFrozen: true,
);
```

| Type | Definition |
|---|---|
| List, set | `['type' => Type::LIST, 'valueType' => …, 'isFrozen' => bool]` |
| Map | `['type' => Type::MAP, 'keyType' => …, 'valueType' => …, 'isFrozen' => bool]` |
| Tuple | `['type' => Type::TUPLE, 'valueTypes' => [ … ]]` |
| UDT | `['type' => Type::UDT, 'valueTypes' => ['field' => …], 'isFrozen' => bool]` |
| Vector | `['type' => Type::VECTOR, 'valueType' => …, 'dimensions' => int]` |

Maps
----

A PHP array key can only be an `int` or a `string`, so a CQL map cannot always become an array. `MapEncodeOption` decides what you get when reading:

| Option | Result |
|---|---|
| `AUTO` (default) | An array when the keys fit, otherwise a `MapCollection` |
| `AS_ARRAY` | Always an array; raises `ValueException` if the keys do not fit |
| `AS_MAP_COLLECTION` | Always a `MapCollection` |

The choice depends only on the column's key type and your encoding options, never on the data, so a given column always has the same PHP type. Keys that are text, numbers, UUIDs and the like fit. Tuple, UDT and collection keys do not, nor do temporal keys configured as `DateTimeImmutable`.

```php
use Cassandra\Value\MapCollection;

// With a tuple key, or a timestamp key configured as DateTimeImmutable:
$map = $row['deployments'];
assert($map instanceof MapCollection);

foreach ($map->getEntries() as $entry) {
    var_dump($entry->key, $entry->value);
}
```

To write such a map, list its entries:

```php
use Cassandra\Type;
use Cassandra\Value\MapCollection;
use Cassandra\Value\MapEntry;

$deployments = MapCollection::fromEntries(
    [new MapEntry(new DateTimeImmutable('2026-08-12T09:00:00Z'), 'production')],
    Type::TIMESTAMP,
    Type::VARCHAR,
);

$byTuple = MapCollection::fromEntries(
    [new MapEntry([7, 'seven'], 'value')],
    ['type' => Type::TUPLE, 'valueTypes' => [Type::INT, Type::VARCHAR]],
    Type::VARCHAR,
);
```

Reading values
--------------

Choose how values are returned for the whole connection or for one result:

```php
use Cassandra\Value\EncodeOption\DateEncodeOption;
use Cassandra\Value\EncodeOption\TimestampEncodeOption;
use Cassandra\Value\EncodeOption\UuidEncodeOption;
use Cassandra\Value\ValueEncodeConfig;

$config = new ValueEncodeConfig(
    timestampEncodeOption: TimestampEncodeOption::AS_INT,
    dateEncodeOption: DateEncodeOption::AS_DATETIME_IMMUTABLE,
    uuidEncodeOption: UuidEncodeOption::AS_BINARY,
);

$conn->configureValueEncoding($config);

$rows = $conn->query('SELECT * FROM ks.events')->asRowsResult();
$rows->configureValueEncoding($config);
```

| Option | Default | Alternatives |
|---|---|---|
| `dateEncodeOption` | `AS_STRING` | `AS_INT`, `AS_DATETIME_IMMUTABLE` |
| `timeEncodeOption` | `AS_STRING` | `AS_INT`, `AS_DATETIME_IMMUTABLE` |
| `timestampEncodeOption` | `AS_STRING` | `AS_INT`, `AS_DATETIME_IMMUTABLE` |
| `durationEncodeOption` | `AS_STRING` | `AS_DATEINTERVAL`, `AS_DATEINTERVAL_STRING`, `AS_NATIVE_VALUE` |
| `uuidEncodeOption` | `AS_STRING` | `AS_BINARY`, which is faster for large results and can be bound again as is |
| `varintEncodeOption` | `AS_STRING` | `AS_INT` |
| `mapEncodeOption` | `AUTO` | See [Maps](#maps) |

Collection updates
------------------

On non-frozen collections, write the `UPDATE` in CQL and bind only the change. With the default `autoPrepare` plain PHP arrays work.

| Operation | `set` | `list` | `map` |
|---|---|---|---|
| Add | `col = col + ?` | `col = col + ?` to append, `col = ? + col` to prepend | `col = col + ?` |
| Remove | `col = col - ?` | `col = col - ?`, every occurrence of each value | `col = col - ?` with a set of keys |
| One element | | `col[?] = ?`, `DELETE col[?]` by index | `col[?] = ?`, `DELETE col[?]` by key |
| Clear | `col = {}` | `col = ?` with `[]` | `col = {}` |

```php
$conn->query('UPDATE ks.users SET tags = tags + ? WHERE id = ?', [['cassandra'], 1]);
$conn->query('UPDATE ks.users SET phones = ? + phones WHERE id = ?', [['555-0001'], 1]);
$conn->query('UPDATE ks.teams SET members[?] = ? WHERE id = ?', [2006, 'Ada', 1]);
$conn->query('UPDATE ks.teams SET members = members - ? WHERE id = ?', [[2013, 2014], 1]);
$conn->query('UPDATE ks.stats SET hits = hits + ? WHERE id = ?', [5, 1]); // counters too
```

- Bind the collection as one `?`: `{ ? }` and `[ ? ]` are not valid CQL.
- An empty collection is stored as `null`.
- Assigning `null` to a map key deletes it.
- List operations by index need a read on the server and are unsafe under concurrent writes.
- Frozen collections cannot use `+` or `-`: assign the whole value.

Lightweight transactions
------------------------

A conditional statement returns rows whose `[applied]` column says whether it took effect:

```php
use Cassandra\Consistency;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\SerialConsistency;

$row = $conn->query(
    'INSERT INTO ks.users (id, name) VALUES (?, ?) IF NOT EXISTS',
    [1, 'Ada'],
    Consistency::QUORUM,
    new QueryOptions(serialConsistency: SerialConsistency::LOCAL_SERIAL),
)->asRowsResult()->fetch();

if ($row['[applied]'] !== true) {
    echo 'Already taken by ', $row['name'], "\n"; // the conflicting row comes back
}
```

Read with `Consistency::SERIAL` or `LOCAL_SERIAL` to see transactions in progress. They cost about four extra round trips, so use them only where needed. A batch can hold conditional statements for one partition.

JSON
----

JSON travels as plain text, so no special API is needed:

```php
$conn->query('INSERT INTO ks.users JSON ?', [json_encode(['id' => 1, 'name' => 'Ada'])]);
$conn->query('INSERT INTO ks.users JSON ? DEFAULT UNSET', ['{"id": 1, "age": 30}']); // keep other columns

$row = $conn->query('SELECT JSON * FROM ks.users WHERE id = ?', [1])->asRowsResult()->fetch();
$data = json_decode($row['[json]'], true);
```

Async requests
--------------

Each async call sends its request at once and returns a `Statement`. All requests share one connection, so this runs many queries concurrently without threads. PHP still does one thing at a time: answers are read when you wait or poll.

### Run many queries, then use the results

```php
$statements = [];
foreach ($userIds as $id) {
    $statements[$id] = $conn->queryAsync('SELECT * FROM ks.users WHERE id = ?', [$id]);
}

$conn->waitForStatements($statements);

foreach ($statements as $id => $statement) {
    $users[$id] = $statement->getRowsResult()->fetch();
}
```

### Handle each result as it arrives

`waitForAnyStatement()` keeps returning a ready statement, so remove it from the list:

```php
while ($pending !== []) {
    $ready = $conn->waitForAnyStatement($pending);
    $pending = array_filter($pending, static fn ($statement) => $statement !== $ready);

    handle($ready->getRowsResult());
}
```

### Limit how many run at once

Sending a large number of writes without waiting overloads the cluster, so keep a window:

```php
$inFlight = [];

foreach ($rows as $row) {
    if (count($inFlight) >= 100) {
        $ready = $conn->waitForAnyStatement($inFlight);
        $inFlight = array_filter($inFlight, static fn ($statement) => $statement !== $ready);
        $ready->getResult(); // raises if this write failed
    }

    $inFlight[] = $conn->executeAsync($insert, $row);
}

$conn->waitForStatements($inFlight);
foreach ($inFlight as $statement) {
    $statement->getResult();
}
```

### Poll from your own loop

These calls never wait:

```php
$statement = $conn->queryAsync('SELECT * FROM ks.big_table');

while (!$conn->tryResolveStatement($statement)) {
    doOtherWork();
}

$rows = $statement->getRowsResult();
```

`tryResolveStatements()` does the same for a list, and `drainAvailableResponses()` processes whatever has arrived.

### Prepared statements

```php
$prepared = $conn->prepareAsync('SELECT name FROM ks.users WHERE id = ?')->getPreparedResult();
$statement = $conn->executeAsync($prepared, [42]);
$rows = $statement->getRowsResult();
```

### Failures

Getting the result of a `Statement` raises its failure. One failed or timed-out statement does not affect the others:

```php
use Cassandra\Exception\RequestTimeoutException;
use Cassandra\Exception\StatementException;

try {
    $conn->waitForStatements($statements);
} catch (RequestTimeoutException $e) {
    foreach ($e->getTimedOutStatements() as $timedOut) {
        $conn->asyncRequest($timedOut->getOriginalRequest()); // only if safe to run twice
    }
}

try {
    $rows = $statement->getRowsResult();
} catch (StatementException $e) {
    // $statement->isAbandoned(): the connection closed while it was in flight; send it again.
}
```

Timeouts
--------

Three settings cover nearly every case:

| Setting | Default | Controls |
|---|---|---|
| `requestTimeoutInSeconds` | 30s | How long the server may take to answer. Raises `RequestTimeoutException` and leaves the connection open. |
| `heartbeatIntervalInSeconds` | 30s | How quickly a dead connection is noticed: after interval + `heartbeatTimeoutInSeconds`, about 35s. |
| `$timeoutInSeconds` of a `waitFor…()` call | see below | How long that call may block. Returns empty-handed; nothing is given up on. |

### Slow operations

The default sits above Cassandra's own coordinator timeouts, so that the server can answer with a proper error first. Raise it for operations that take longer. A connection that dies is still noticed by the heartbeat, so a high value is safe.

```php
use Cassandra\Request\Options\BatchOptions;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Request\Query;

// One request. The request's own option wins over the connection's setting.
$conn->query('TRUNCATE ks.big_table', options: new QueryOptions(requestTimeoutInSeconds: 90.0));
$batch = $conn->createBatchRequest(options: new BatchOptions(requestTimeoutInSeconds: 60.0));
$conn->syncRequest(new Query('TRUNCATE ks.t', options: new QueryOptions(requestTimeoutInSeconds: 90.0)));

// Everything from now on.
$conn->setRequestTimeout(120.0);
```

| Operation | Suggested |
|---|---|
| Ordinary reads and writes | 30s, the default |
| `TRUNCATE` (60s on the server) | 90s |
| Schema changes | 120s |
| Full scans, aggregates, large batches | 300s or more |

`null` falls back to the next level, and at the connection level, like `INF`, means no limit. The timeout applies to each request sent, so a statement that must be prepared first gets it twice, and `queryAll()` gets it per page. An async request's time runs from when it was sent.

### Waiting

| Value of `$timeoutInSeconds` | Meaning |
|---|---|
| `Connection::WAIT_DEFAULT` (`null`) | The default of that call, below |
| `Connection::WAIT_NONE` (`0.0`) | One non-blocking read attempt |
| A number | At most that many seconds |
| `Connection::WAIT_FOREVER` (`INF`) | As long as it takes |

| Call | Default |
|---|---|
| `waitForStatements()`, `waitForAnyStatement()`, `waitForAllPendingStatements()` | Until the statements' own request timeouts |
| `waitForNextResponse()` | The connection's request timeout |
| `waitForNextEvent()` | As long as it takes |

`waitForNextEvent()` and `waitForNextResponse()` return `null` when the time is up. A request that expires during them raises when you next use its `Statement`, so an event loop is not interrupted for it.

### Which exception means what

| Exception | Raised by | Meaning | Connection |
|---|---|---|---|
| `ServerException\ReadTimeoutException`, `WriteTimeoutException` | Server | Replicas did not answer the coordinator in time | Stays open |
| `RequestTimeoutException` | Client | The server did not answer within the request timeout | Stays open |
| `ConnectionException` | Client | The heartbeat went unanswered, or too many requests timed out (`maxOrphanedStreams`) | Replaced |
| `SocketException`, `StreamException` | Transport | No progress within the transport timeout, and nothing else bounded the wait | Replaced |

### Transport timeouts

These bound how long a connection makes no progress at all, not how long a request takes, so large results on slow links do not trip them. You rarely need to change them, and deadlines fire on time whatever they are. Zero disables them, which is safe while the heartbeat is on.

When requests keep timing out, the connection is replaced after `maxOrphanedStreams` of them, since a late answer holds on to its stream id. That raises a `ConnectionException`, and statements in flight are marked abandoned.

Events and warnings
-------------------

Register a listener, then subscribe. A subscription ends with the connection, so send `Register` again after a reconnect.

```php
use Cassandra\EventListener;
use Cassandra\EventType;
use Cassandra\Request\Register;
use Cassandra\Response\Event;
use Cassandra\Response\Event\SchemaChangeEvent;

$conn->registerEventListener(new class () implements EventListener {
    public function onEvent(Event $event): void {
        if ($event instanceof SchemaChangeEvent) {
            error_log('Schema changed in ' . $event->getData()->keyspace);
        }
    }
});

$conn->syncRequest(new Register([
    EventType::SCHEMA_CHANGE,
    EventType::STATUS_CHANGE,
    EventType::TOPOLOGY_CHANGE,
]));

while (true) {
    $conn->waitForNextEvent(); // or tryReadNextEvent() inside your own loop
}
```

Warnings the server attaches to a response go to a warnings listener:

```php
use Cassandra\Request\Request;
use Cassandra\Response\Response;
use Cassandra\WarningsListener;

$conn->registerWarningsListener(new class () implements WarningsListener {
    public function onWarnings(array $warnings, Request $request, Response $response): void {
        error_log('Cassandra: ' . implode('; ', $warnings));
    }
});
```

Listeners run inside whichever read received the frame, so keep them short. An exception from a listener reaches the caller as a `ConnectionException` once the frame has been handled.

Errors
------

All exceptions extend `Cassandra\Exception\CassandraException`:

| Exception | Meaning |
|---|---|
| `ServerException` | The server answered with an error; subclasses below |
| `RequestTimeoutException` | No answer within the request timeout |
| `ConnectionException` | Connecting, handshake or authentication failed, or the connection was replaced |
| `NodeException` | Transport failure: `SocketException` or `StreamException` |
| `StatementException` | A statement cannot deliver: abandoned, or a different result type was requested |
| `RequestException`, `ValueException`, `ValueFactoryException`, `TypeInfoException`, `TypeNameParserException` | Invalid input |
| `ResponseException`, `CompressionException`, `VIntCodecException`, `StringMathException` | Undecodable data or a failed calculation |

`ServerException` subclasses, in `Cassandra\Exception\ServerException`: `AlreadyExistsException`, `AuthenticationErrorException`, `CasWriteUnknownException`, `CdcWriteFailureException`, `ConfigErrorException`, `FunctionFailureException`, `InvalidException`, `IsBootstrappingException`, `OverloadedException`, `ProtocolErrorException`, `ReadFailureException`, `ReadTimeoutException`, `ServerErrorException`, `SyntaxErrorException`, `TruncateErrorException`, `UnauthorizedException`, `UnavailableException`, `UnpreparedException`, `WriteFailureException`, `WriteTimeoutException`.

`getCode()` is a stable `ExceptionCode` value, or the protocol's error code on a `ServerException`, and `getContext()` adds details. Server exceptions also carry `getErrorContext()`:

```php
use Cassandra\Exception\ServerException\ReadTimeoutException;
use Cassandra\Exception\ServerException\UnavailableException;

try {
    $conn->query('SELECT * FROM ks.users');
} catch (UnavailableException $e) {
    $context = $e->getErrorContext();
    error_log("{$context->nodesAlive} of {$context->nodesRequired} replicas alive");
} catch (ReadTimeoutException $e) {
    $context = $e->getErrorContext();
    error_log("{$context->nodesAnswered} of {$context->nodesRequired} replicas answered");
}
```

### Retrying

The driver does not retry for you, because only you know whether an operation is safe to run twice. After a lost connection the failing call raises and the next call reconnects, so requests in flight are never re-sent silently. Retry idempotent operations on transient errors, with a backoff:

```php
use Cassandra\Exception\RequestTimeoutException;
use Cassandra\Exception\ServerException\OverloadedException;
use Cassandra\Exception\ServerException\ReadTimeoutException;
use Cassandra\Exception\ServerException\UnavailableException;
use Cassandra\Exception\ServerException\WriteTimeoutException;

function withRetry(callable $operation, int $attempts = 3): mixed {
    for ($attempt = 1; ; $attempt++) {
        try {
            return $operation();
        } catch (UnavailableException|ReadTimeoutException|WriteTimeoutException|OverloadedException|RequestTimeoutException $e) {
            if ($attempt >= $attempts) {
                throw $e;
            }

            usleep(random_int(50_000, 100_000 * 2 ** $attempt));
        }
    }
}
```

Compression
-----------

Set `enableCompression: true` in `ConnectionOptions`. If the server supports LZ4, requests and responses are compressed. It works out of the box and uses the [`lz4` extension](https://github.com/kjdev/php-ext-lz4) automatically when installed, which is much faster. A server that offers no compression leaves the connection uncompressed, and one that offers only other algorithms raises a `ConnectionException`.

Tracing and custom payloads
---------------------------

```php
use Cassandra\Request\Query;

$request = new Query('SELECT now() FROM system.local');
$request->enableTracing();
$request->setPayload(['my-key' => 'my-value']); // v4+

$response = $conn->syncRequest($request);
echo $response->getTracingUuid();
```

Tracing adds overhead, so use it sparingly in production.

Server compatibility
--------------------

| Server | Protocol | Configuration |
|---|---|---|
| Cassandra 5.x, 4.x | v5 or v4, negotiated | None |
| Cassandra 3.x, 2.2 | v4 | None |
| Cassandra 2.1 | v3 | `new ConnectionOptions(initialProtocolVersion: ProtocolVersion::V3)` |
| ScyllaDB 6.2, 2025.x, 2026.x | v4 | None |

Cassandra 3.0, 3.11, 4.0, 4.1 and 5.0 and ScyllaDB 6.2, 2025.1, 2026.1 and 2026.2 are in the test matrix. ScyllaDB's own protocol extensions, such as shard awareness, are not used.

Protocol v4 has no checksums, so without TLS a bit flipped on the network can reach your application as wrong data. Protocol v5 checksums every frame and raises an exception instead. LZ4 on v4 does not close this gap. Use TLS wherever the network is not fully trusted, which matters most on ScyllaDB and older Cassandra, where v4 is the newest protocol.

Some CQL features must be enabled on the server:

| Feature | Cassandra (`cassandra.yaml`) | ScyllaDB |
|---|---|---|
| User-defined functions | `user_defined_functions_enabled: true` | `--experimental-features=udf --enable-user-defined-functions=true` |
| Materialized views | `materialized_views_enabled: true` | Enabled |
| SASI indexes | `sasi_indexes_enabled: true` | Not supported |

In Cassandra 4.0 the settings are called `enable_user_defined_functions`, `enable_materialized_views` and `enable_sasi_indexes`.

Migrating from the DataStax driver
----------------------------------

| DataStax | php-cassandra |
|---|---|
| `Cassandra::cluster()->withContactPoints(…)->build()->connect('ks')` | `new Connection([new StreamNodeConfig(…)], keyspace: 'ks')` then `connect()` |
| `$session->execute('SELECT …', ['arguments' => [$id]])` | `$conn->query('SELECT …', [$id])->asRowsResult()` |
| `$session->prepare('…')` | `$conn->prepare('…')` |
| `$session->execute($prepared, ['arguments' => [$id]])` | `$conn->execute($prepared, [$id])->asRowsResult()` |
| `$session->executeAsync(…)->get()` | `$conn->queryAsync(…)->getRowsResult()` |
| `new Cassandra\Uuid('…')` | `Uuid::fromValue('…')`, or just the string when prepared |
| `new Cassandra\BatchStatement(…)` | `$conn->createBatchRequest(…)` |

Benchmarks
----------

Compared with the C-extension drivers, using `benchmarks/` against one Cassandra node. Lower is better, and times are per iteration.

| Benchmark | php-cassandra (PHP 8.5) | DataStax (PHP 7.1) | ScyllaDB (PHP 8.5) |
|---|---|---|---|
| Insert and select, no type info | 3.90s | 6.82s | 7.48s |
| Insert and select, with type info | 3.83s | 6.61s | 6.88s |
| Paged query | 252ms | 562ms | 554ms |
| Prepared insert | 1.82s | 3.09s | 3.02s |
| Simple select | 152ms | 329ms | 359ms |

For hot paths, use prepared statements and iterate results rather than building large arrays. See `benchmarks/README.md` to reproduce the results.

API overview
------------

**`Cassandra\Connection`**

| Purpose | Methods |
|---|---|
| Connection | `connect()`, `disconnect()`, `isConnected()`, `getProtocolVersion()`, `supportsKeyspaceRequestOption()`, `supportsNowInSecondsRequestOption()` |
| Settings | `setConsistency()`, `getConsistency()`, `withConsistency()`, `setKeyspace()`, `getKeyspace()`, `withKeyspace()`, `setRequestTimeout()`, `getRequestTimeout()`, `configureValueEncoding()` |
| Sending | `query()`, `queryAll()`, `queryAsync()`, `prepare()`, `prepareAsync()`, `execute()`, `executeAll()`, `executeAsync()`, `createBatchRequest()`, `batch()`, `batchAsync()`, `syncRequest()`, `asyncRequest()` |
| Waiting | `waitForStatements()`, `waitForAnyStatement()`, `waitForAllPendingStatements()`, `waitForNextResponse()`, `waitForNextEvent()`, `getResponseForStatement()` |
| Polling | `tryResolveStatement()`, `tryResolveStatements()`, `tryReadNextResponse()`, `tryReadNextEvent()`, `drainAvailableResponses()` |
| Listeners | `registerEventListener()`, `unregisterEventListener()`, `registerWarningsListener()`, `unregisterWarningsListener()` |

**`Cassandra\Statement`**: `getResult()`, `getRowsResult()`, `getPreparedResult()`, `getSchemaChangeResult()`, `getSetKeyspaceResult()`, `getResponse()`, `waitForResponse()`, the non-blocking `tryGet…()` forms of each, `peekResponse()`, `isResultReady()`, `isWaitingForResult()`, `isTimedOut()`, `isAbandoned()`, `getRequestTimeout()`, `getOriginalRequest()`.

**`Cassandra\Response\Result`**: `asRowsResult()`, `asPreparedResult()`, `asSchemaChangeResult()`, `asSetKeyspaceResult()`, `asVoidResult()`.

**Extension points**: `Connection\Authenticator`, `Connection\NodeSelector`, `Connection\NodeConfig`, `Connection\IoNode`, `EventListener`, `WarningsListener`, `Response\Result\RowClassInterface`. Implementations must throw from `Cassandra\Exception`.

Classes marked `@internal` can change in any release.

Contributing
------------

```bash
git clone https://github.com/MichaelRoosz/php-cassandra.git && cd php-cassandra
composer install

composer lint && composer phpstan && composer psalm
composer test:unit
composer test:integration   # needs Docker: starts Cassandra, runs the suite, cleans up
```

See `CONTRIBUTING.md` for how to contribute, `SECURITY.md` for reporting vulnerabilities and `RELEASING.md` for the release process.

Maintained by Michael Roosz, building on the earlier work of Shen Zhenyu and Evseev Nikolay. Released under the [MIT License](LICENSE). See `CHANGELOG.md` for release notes.
