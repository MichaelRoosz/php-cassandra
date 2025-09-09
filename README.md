php-cassandra: A modern Cassandra client for PHP
================================================

[![Latest Stable Version](https://poser.pugx.org/mroosz/php-cassandra/v/stable)](https://packagist.org/packages/mroosz/php-cassandra)
[![License](https://poser.pugx.org/mroosz/php-cassandra/license)](https://packagist.org/packages/mroosz/php-cassandra)
[![PHP Version Require](https://poser.pugx.org/mroosz/php-cassandra/require/php)](https://packagist.org/packages/mroosz/php-cassandra)
[![Total Downloads](https://poser.pugx.org/mroosz/php-cassandra/downloads)](https://packagist.org/packages/mroosz/php-cassandra)

php-cassandra is a pure-PHP client for Apache Cassandra with support for CQL binary protocol v3, v4 and v5 (Cassandra 4.x/5.x), synchronous and asynchronous APIs, prepared statements, batches, result iterators, object mapping, SSL/TLS, and LZ4 compression.

**Package:** https://packagist.org/packages/mroosz/php-cassandra  
**Repository:** https://github.com/MichaelRoosz/php-cassandra

Table of contents
-----------------

- [php-cassandra: A modern Cassandra client for PHP](#php-cassandra-a-modern-cassandra-client-for-php)
  - [Table of contents](#table-of-contents)
  - [Introduction](#introduction)
    - [Why choose php-cassandra?](#why-choose-php-cassandra)
    - [Key Features](#key-features)
  - [Requirements](#requirements)
    - [System Requirements](#system-requirements)
    - [PHP Extensions](#php-extensions)
    - [Data Type Compatibility](#data-type-compatibility)
  - [Installation](#installation)
    - [Using Composer (Recommended)](#using-composer-recommended)
    - [Without Composer](#without-composer)
  - [Quick start](#quick-start)
    - [Basic Connection and Query](#basic-connection-and-query)
    - [Prepared statements](#prepared-statements)
    - [Async Operations Example](#async-operations-example)
    - [Error Handling Example](#error-handling-example)
    - [SSL/TLS Connection Example](#ssltls-connection-example)
  - [Connecting](#connecting)
  - [Consistency levels](#consistency-levels)
  - [Queries](#queries)
  - [Prepared statements](#prepared-statements-1)
  - [Batches](#batches)
  - [Results and fetching](#results-and-fetching)
  - [Object mapping](#object-mapping)
  - [Data types](#data-types)
  - [Type definition syntax for complex values](#type-definition-syntax-for-complex-values)
  - [Events](#events)
  - [Tracing and custom payloads (advanced)](#tracing-and-custom-payloads-advanced)
  - [Asynchronous API](#asynchronous-api)
  - [Compression](#compression)
  - [Error handling](#error-handling)
    - [Exception Hierarchy](#exception-hierarchy)
    - [Error Handling Patterns](#error-handling-patterns)
      - [Basic Error Handling](#basic-error-handling)
      - [Specific Server Error Handling](#specific-server-error-handling)
      - [Retry Logic with Exponential Backoff](#retry-logic-with-exponential-backoff)
      - [Timeout Handling](#timeout-handling)
    - [Error Information Access](#error-information-access)
  - [Configuration Reference](#configuration-reference)
    - [Connection Configuration](#connection-configuration)
      - [Node Configuration](#node-configuration)
      - [Connection Options](#connection-options)
    - [Request Options](#request-options)
      - [Query Options](#query-options)
      - [Execute Options](#execute-options)
      - [Prepare Options](#prepare-options)
      - [Batch Options](#batch-options)
    - [Advanced Configuration](#advanced-configuration)
      - [Value Encoding Configuration](#value-encoding-configuration)
      - [Event Listeners](#event-listeners)
  - [Notes](#notes)
  - [Frequently Asked Questions (FAQ)](#frequently-asked-questions-faq)
    - [General Questions](#general-questions)
    - [Installation and Setup](#installation-and-setup)
    - [Data Types and Modeling](#data-types-and-modeling)
  - [Migration Guide](#migration-guide)
    - [From DataStax PHP Driver](#from-datastax-php-driver)
      - [Connection Setup](#connection-setup)
      - [Query Execution](#query-execution)
      - [Prepared Statements](#prepared-statements-2)
      - [Data Types](#data-types-1)
      - [Async Operations](#async-operations)
    - [Migration Checklist](#migration-checklist)
  - [Examples](#examples)
  - [Connection tuning examples](#connection-tuning-examples)
  - [Configuring value encoding](#configuring-value-encoding)
  - [Warnings listener](#warnings-listener)
  - [Event processing patterns](#event-processing-patterns)
  - [v5 keyspace per request](#v5-keyspace-per-request)
  - [Tracing notes](#tracing-notes)
  - [Performance tips](#performance-tips)
  - [Version support](#version-support)
  - [API reference (essentials)](#api-reference-essentials)
  - [Contributing](#contributing)
    - [Development Setup](#development-setup)
    - [Contribution Guidelines](#contribution-guidelines)
      - [Code Standards](#code-standards)
    - [Contributors](#contributors)
    - [Supporting the Project](#supporting-the-project)

Introduction
------------

php-cassandra is a modern PHP client for Apache Cassandra that prioritizes **correctness**, **performance**, and **developer experience**. This library aims to provide full protocol coverage and advanced features while maintaining simplicity.

### Why choose php-cassandra?

**🚀 Modern Architecture**
- Pure PHP implementation with no external dependencies
- Support for latest Cassandra protocol versions (v3/v4/v5)
- Built for PHP 8.1+ with modern language features

**⚡ High Performance**
- Asynchronous request pipelining for maximum throughput
- LZ4 compression support for reduced bandwidth
- Prepared statement caching and reuse

**🎯 Developer Friendly**
- Complete data type coverage including complex nested structures
- Rich configuration options with sensible defaults
- Object mapping with customizable row classes

### Key Features

- **Protocol Support**: v3/v4/v5 with automatic negotiation
- **Transports**: Sockets and PHP streams (SSL/TLS, persistent connections)
- **Request Types**: Synchronous, Asynchronous
- **Statements**: Prepared statements with positional/named binding, auto-prepare
- **Data Types**: Full coverage including collections, tuples, UDTs, custom types, vectors
- **Results**: Iterators, multiple fetch styles, object mapping
- **Events**: Schema/status/topology change notifications
- **Advanced**: LZ4 compression, server overload signalling, tracing support


Requirements
------------

### System Requirements

| Component | Minimum | Recommended | Notes |
|-----------|---------|-------------|-------|
| **PHP Version** | 8.1.0 | 8.3+ | Latest stable version recommended |
| **Architecture** | 32-bit/64-bit | 64-bit | 64-bit required for full type support |
|
### PHP Extensions

| Extension | Required | Purpose | Notes |
|-----------|----------|---------|-------|
| **sockets** | Optional | Socket transport | Required for sockets connection confiured with `SocketNodeConfig` |
| **bcmath** or **gmp** | Optional | Provides performance improvement for large integer operations | For `Varint` and `Decimal` types |
| **openssl** | Optional | SSL/TLS connections | Required for encrypted connections |
|
### Data Type Compatibility

Some Cassandra data types require 64-bit PHP for proper handling:

| Type | 32-bit PHP | 64-bit PHP | Notes |
|------|------------|------------|-------|
| `Bigint` | ⚠️ Limited | ✅ Full | Values > 2^31 may lose precision |
| `Counter` | ⚠️ Limited | ✅ Full | Same as Bigint |
| `Duration` | ⚠️ Limited | ✅ Full | Nanosecond precision requires 64-bit |
| `Time` | ⚠️ Limited | ✅ Full | Nanosecond precision requires 64-bit |
| `Timestamp` | ⚠️ Limited | ✅ Full | Millisecond precision may be affected |

Installation
------------

### Using Composer (Recommended)

```bash
composer require mroosz/php-cassandra
```

### Without Composer

If you can't use Composer, you can load the library's own autoloader:

```php
<?php
require __DIR__ . '/php-cassandra/php-cassandra.php';
```

Quick start
-----------

### Basic Connection and Query

```php
<?php

use Cassandra\Connection;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Consistency;

// Connect to Cassandra
$nodes = [
    new StreamNodeConfig(
        host: '127.0.0.1', 
        port: 9042, 
        username: 'cassandra', 
        password: 'cassandra'
    ),
];

$conn = new Connection($nodes, keyspace: 'my_keyspace');
$conn->connect();
$conn->setConsistency(Consistency::QUORUM);

// Simple query
$result = $conn->query('SELECT * FROM system.local')->asRowsResult();
foreach ($result as $row) {
    echo "Cluster: " . $row['cluster_name'] . "\n";
}
```

### Prepared statements

**todo**

### Async Operations Example

```php
<?php
use Cassandra\Request\Options\QueryOptions;

// Fire multiple queries concurrently
$statements = [];
$statements[] = $conn->queryAsync(
    'SELECT COUNT(*) FROM users', 
    options: new QueryOptions(pageSize: 1000)
);
$statements[] = $conn->queryAsync(
    'SELECT * FROM users LIMIT 10',
    options: new QueryOptions(pageSize: 10)
);

// Process results as they become available
$userCount = $statements[0]->getRowsResult()->fetch()['count'];
$recentUsers = $statements[1]->getRowsResult()->fetchAll();

echo "Total users: {$userCount}\n";
echo "Recent users: " . count($recentUsers) . "\n";
```

### Error Handling Example

```php
<?php
use Cassandra\Response\Exception as ServerException;
use Cassandra\Connection\Exception as ConnectionException;
use Cassandra\Exception as CassandraException;

try {
    $result = $conn->query(
        'SELECT * FROM users WHERE email = ?',
        ['john.doe@example.com'],
        Consistency::LOCAL_QUORUM
    )->asRowsResult();
    
    foreach ($result as $user) {
        echo "Found user: {$user['name']}\n";
    }
    
} catch (ServerException $e) {
    echo "Server error: " . $e->getMessage() . "\n";
    // Handle specific server errors (timeouts, unavailable nodes, etc.)
    
} catch (ConnectionException $e) {
    echo "Connection error: " . $e->getMessage() . "\n";
    // Handle network/connection issues
    
} catch (CassandraException $e) {
    echo "Client error: " . $e->getMessage() . "\n";
    // Handle client-side errors
}
```

### SSL/TLS Connection Example

```php
<?php
use Cassandra\Connection\StreamNodeConfig;

// Secure connection with TLS
$secureNode = new StreamNodeConfig(
    host: 'tls://cassandra.example.com',
    port: 9042,
    username: 'secure_user',
    password: 'secure_password',
    sslOptions: [
        'cafile' => '/path/to/ca.pem',
        'verify_peer' => true,
        'verify_peer_name' => true,
    ]
);

$conn = new Connection([$secureNode], keyspace: 'production_app');
$conn->connect();

echo "Secure connection established!\n";
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
    // see https://www.php.net/manual/en/function.socket-get-option.php
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
```
**todo: add more examples here**

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
use Cassandra\Value\Blob;
use Cassandra\Value\Boolean;
// counter
// custom
use Cassandra\Value\Date;
use Cassandra\Value\Decimal;
use Cassandra\Value\Double;
use Cassandra\Value\Duration;
use Cassandra\Value\Float32;
use Cassandra\Value\Inet;
use Cassandra\Value\Int32;
use Cassandra\Value\ListCollection;
use Cassandra\Value\MapCollection;
use Cassandra\Value\SetCollection;
use Cassandra\Value\Smallint;
use Cassandra\Value\Time;
use Cassandra\Value\Timestamp;
use Cassandra\Value\Timeuuid;
use Cassandra\Value\Tinyint;
use Cassandra\Value\Tuple;
use Cassandra\Value\UDT;
use Cassandra\Value\Uuid;
use Cassandra\Value\Varchar;
use Cassandra\Value\Varint;
use Cassandra\Value\Vector;
use Cassandra\Type;

// Scalars
Ascii::fromValue('hello');
Bigint::fromValue(10_000_000_000);
Blob::fromValue("\xFF\xFF");
Boolean::fromValue(true);
// todo: counter
// todo: custom
// todo: decimal
Double::fromValue(2.718281828459);
Float32::fromValue(2.718);
// todo: inet
Int32::fromValue(-123);
Smallint::fromValue(2048);
// todo: timeuuid
Tinyint::fromValue(12);
// todo: uuid
// todo: varchar
Varint::fromValue(10000000000);

// Temporal
Date::fromValue('2011-02-03');
Duration::fromValue('89h4m48s');
Time::fromValue('08:12:54.123456789');
Timestamp::fromValue('2011-02-03T04:05:00.000+0000');

// Collections / Tuples / UDT / Vector
ListCollection::fromValue([1, 2, 3], Type::INT);
MapCollection::fromValue(['a' => 1], Type::ASCII, Type::INT);
SetCollection::fromValue([1, 2, 3], Type::INT);
Tuple::fromValue([1, 'x'], [Type::INT, Type::VARCHAR]);
UDT::fromValue(['id' => 1, 'name' => 'n'], ['id' => Type::INT, 'name' => Type::VARCHAR]);
Vector::fromValue([0.12, -0.3, 0.9], Type::FLOAT, dimensions: 3);
```

Type definition syntax for complex values
----------------------------------------

For complex types, the driver needs a type definition to encode PHP values. Wherever you see a parameter like `\Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>)`, you can either pass a scalar `Type::...` (for simple elements) or a definition array with nested types for complex structures. The common shapes are:

- List: `['type' => Type::LIST, 'valueType' => <elementType>, 'isFrozen' => bool]`
- Set: `['type' => Type::SET, 'valueType' => <elementType>, 'isFrozen' => bool]`
- Map: `['type' => Type::MAP, 'keyType' => <keyType>, 'valueType' => <valueType>, 'isFrozen' => bool]`
- Tuple: `['type' => Type::TUPLE, 'valueTypes' => [<t1>, <t2>, ...]]`
- UDT: `['type' => Type::UDT, 'valueTypes' => ['field' => <type>, ...], 'isFrozen' => bool, 'keyspace' => 'ks', 'name' => 'udt_name']`
**todo: add vector**

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
        'valueTypes' => [
            'id' => Type::INT,
            'name' => Type::VARCHAR,
            'active' => Type::BOOLEAN,
            'friends' => [
                'type' => Type::LIST, 
                'valueType' => Type::VARCHAR
            ],
            'drinks' => [
                'type' => Type::LIST, 
                'valueType' => [
                    'type' => Type::UDT,
                    'valueTypes' => [
                        'qty' => Type::INT,
                        'brand' => Type::VARCHAR
                    ],
                ]
            ],
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

php-cassandra provides comprehensive error handling with a well-structured exception hierarchy. Understanding these exceptions helps you build robust applications with proper error recovery.

### Exception Hierarchy

```
\Exception
├── \Cassandra\Exception (base client exception)
│   ├── \Cassandra\Connection\Exception (connection/transport errors)
│   │   ├── \Cassandra\Connection\SocketException
│   │   ├── \Cassandra\Connection\StreamException
│   │   └── \Cassandra\Connection\NodeException
│   ├── \Cassandra\Exception\StatementException (result type mismatches)
│   ├── \Cassandra\Exception\TypeNameParserException (type parsing errors)
│   └── \Cassandra\Exception\VIntCodecException (variable integer codec errors)
└── \Cassandra\Response\Exception (server-side errors)
    ├── \Cassandra\Response\Error\ServerError (5xxx errors)
    ├── \Cassandra\Response\Error\ProtocolError (protocol violations)
    ├── \Cassandra\Response\Error\AuthenticationError (authentication failures)
    ├── \Cassandra\Response\Error\Unavailable (insufficient replicas)
    ├── \Cassandra\Response\Error\Overloaded (server overloaded)
    ├── \Cassandra\Response\Error\IsBootstrapping (node bootstrapping)
    ├── \Cassandra\Response\Error\Truncate (truncation errors)
    ├── \Cassandra\Response\Error\WriteTimeout (write timeout)
    ├── \Cassandra\Response\Error\ReadTimeout (read timeout)
    ├── \Cassandra\Response\Error\ReadFailure (read failure)
    ├── \Cassandra\Response\Error\FunctionFailure (UDF/UDA failures)
    ├── \Cassandra\Response\Error\WriteFailure (write failure)
    ├── \Cassandra\Response\Error\Syntax (CQL syntax errors)
    ├── \Cassandra\Response\Error\Unauthorized (permission denied)
    ├── \Cassandra\Response\Error\Invalid (invalid query)
    ├── \Cassandra\Response\Error\Config (configuration errors)
    ├── \Cassandra\Response\Error\AlreadyExists (keyspace/table exists)
    └── \Cassandra\Response\Error\Unprepared (prepared statement not found)
```

### Error Handling Patterns

#### Basic Error Handling
```php
use Cassandra\Exception\StatementException;
use Cassandra\Response\ServerException;
use Cassandra\Connection\Exception as ConnectionException;

try {
    $result = $conn->query('SELECT * FROM users WHERE id = ?', [$userId])
        ->asRowsResult();
    
    foreach ($result as $row) {
        // Process row
    }
    
} catch (ServerException $e) {
    // Server returned an error response
    error_log("Server error: " . $e->getMessage());
    
} catch (ConnectionException $e) {
    // Network/connection issues
    error_log("Connection error: " . $e->getMessage());
    
} catch (StatementException $e) {
    // Wrong result type access (e.g., calling asRowsResult() on non-rows result)
    error_log("Statement error: " . $e->getMessage());
    
} catch (\Cassandra\Exception $e) {
    // Other client-side errors
    error_log("Client error: " . $e->getMessage());
}
```

#### Specific Server Error Handling
```php
use Cassandra\Response\Error\{
    Unavailable, ReadTimeout, WriteTimeout, Overloaded,
    Syntax, Invalid, Unauthorized, AlreadyExists
};

try {
    $conn->query('CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)');
    
} catch (AlreadyExists $e) {
    // Table already exists - this might be OK
    echo "Table already exists, continuing...\n";
    
} catch (Unauthorized $e) {
    // Permission denied
    throw new \RuntimeException("Insufficient permissions: " . $e->getMessage());
    
} catch (Syntax $e) {
    // CQL syntax error
    throw new \RuntimeException("Invalid CQL syntax: " . $e->getMessage());
    
} catch (Invalid $e) {
    // Invalid query (e.g., wrong types)
    throw new \RuntimeException("Invalid query: " . $e->getMessage());
}
```

#### Retry Logic with Exponential Backoff
```php
function executeWithRetry(callable $operation, int $maxRetries = 3): mixed
{
    $attempt = 0;
    $delay = 100; // Start with 100ms
    
    while ($attempt < $maxRetries) {
        try {
            return $operation();
            
        } catch (Unavailable | ReadTimeout | WriteTimeout | Overloaded $e) {
            $attempt++;
            
            if ($attempt >= $maxRetries) {
                throw $e; // Re-throw on final attempt
            }
            
            // Exponential backoff with jitter
            $jitter = rand(0, $delay / 2);
            usleep(($delay + $jitter) * 1000);
            $delay *= 2;
            
            error_log("Retrying operation (attempt {$attempt}/{$maxRetries}) after error: " . $e->getMessage());
            
        } catch (ServerException $e) {
            // Don't retry non-transient errors
            throw $e;
        }
    }
}

// Usage
$result = executeWithRetry(function() use ($conn, $userId) {
    return $conn->query('SELECT * FROM users WHERE id = ?', [$userId])
        ->asRowsResult();
});
```

#### Timeout Handling
```php
use Cassandra\Response\Error\{ReadTimeout, WriteTimeout};

try {
    $result = $conn->query(
        'SELECT * FROM large_table WHERE complex_condition = ?',
        [$condition],
        Consistency::QUORUM
    )->asRowsResult();
    
} catch (ReadTimeout $e) {
    // Handle read timeout
    $consistency = $e->getConsistency();
    $received = $e->getReceived();
    $required = $e->getRequired();
    $dataPresent = $e->isDataPresent();
    
    error_log("Read timeout: got {$received}/{$required} responses at {$consistency}, data_present: " . 
              ($dataPresent ? 'yes' : 'no'));
    
    // Maybe retry with lower consistency
    return $conn->query(
        'SELECT * FROM large_table WHERE complex_condition = ?',
        [$condition],
        Consistency::ONE
    )->asRowsResult();
    
} catch (WriteTimeout $e) {
    // Handle write timeout
    $consistency = $e->getConsistency();
    $received = $e->getReceived();
    $required = $e->getRequired();
    $writeType = $e->getWriteType();
    
    error_log("Write timeout: got {$received}/{$required} responses at {$consistency}, write_type: {$writeType}");
    
    // For BATCH_LOG writes, the operation might have succeeded
    if ($writeType === 'BATCH_LOG') {
        error_log("Batch log write timeout - operation may have succeeded");
    }
}
```

### Error Information Access

Most server exceptions provide additional context:

```php
try {
    $conn->query('SELECT * FROM users');
    
} catch (Unavailable $e) {
    echo "Consistency: " . $e->getConsistency() . "\n";
    echo "Required: " . $e->getRequired() . "\n";
    echo "Alive: " . $e->getAlive() . "\n";
    
} catch (ReadTimeout $e) {
    echo "Consistency: " . $e->getConsistency() . "\n";
    echo "Received: " . $e->getReceived() . "\n";
    echo "Required: " . $e->getRequired() . "\n";
    echo "Data present: " . ($e->isDataPresent() ? 'yes' : 'no') . "\n";
    
} catch (WriteTimeout $e) {
    echo "Write type: " . $e->getWriteType() . "\n";
    echo "Consistency: " . $e->getConsistency() . "\n";
    echo "Received: " . $e->getReceived() . "\n";
    echo "Required: " . $e->getRequired() . "\n";
}
```

Configuration Reference
-----------------------

### Connection Configuration

#### Node Configuration

**SocketNodeConfig** (requires `ext-sockets`)
```php
use Cassandra\Connection\SocketNodeConfig;

$node = new SocketNodeConfig(
    host: '127.0.0.1',                    // Cassandra host
    port: 9042,                           // Cassandra port (default: 9042)
    username: 'cassandra',                // Username (optional)
    password: 'cassandra',                // Password (optional)
    socketOptions: [                      // Socket-specific options
        SO_RCVTIMEO => ['sec' => 10, 'usec' => 0],  // Receive timeout
        SO_SNDTIMEO => ['sec' => 10, 'usec' => 0],  // Send timeout
        SO_KEEPALIVE => 1,                // Keep-alive
    ]
);
```

**StreamNodeConfig** (supports SSL/TLS, persistent connections)
```php
use Cassandra\Connection\StreamNodeConfig;

$node = new StreamNodeConfig(
    host: 'cassandra.example.com',        // Can include protocol (tls://)
    port: 9042,                           // Port number
    username: 'user',                     // Username (optional)
    password: 'secret',                   // Password (optional)
    connectTimeoutInSeconds: 10,          // Connection timeout (default: 5)
    timeoutInSeconds: 30,                 // I/O timeout (default: 30)
    persistent: true,                     // Use persistent connections
    sslOptions: [                         // SSL/TLS options (see PHP SSL context)
        'verify_peer' => true,            // Verify peer certificate
        'verify_peer_name' => true,       // Verify peer name
        'cafile' => '/path/to/ca.pem',    // CA certificate file
        'local_cert' => '/path/to/cert.pem', // Client certificate
        'local_pk' => '/path/to/key.pem', // Client private key
        'passphrase' => 'cert_password',  // Private key passphrase
        'ciphers' => 'HIGH:!aNULL',       // Cipher list
        'crypto_method' => STREAM_CRYPTO_METHOD_TLSv1_2_CLIENT,
    ]
);
```

#### Connection Options

```php
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Connection\NodeSelectionStrategy;

$options = new ConnectionOptions(
    enableCompression: true,              // Enable LZ4 compression (default: false)
    throwOnOverload: true,                // Throw on server overload (v4+, default: false)
    nodeSelectionStrategy: NodeSelectionStrategy::RoundRobin, // Node selection (default: Random)
    preparedResultCacheSize: 200,         // Prepared statement cache size (default: 100)
);
```

### Request Options

#### Query Options
```php
use Cassandra\Request\Options\QueryOptions;
use Cassandra\SerialConsistency;

$queryOptions = new QueryOptions(
    autoPrepare: true,                    // Auto-prepare for type safety (default: true)
    pageSize: 1000,                       // Page size (min 100, default: 5000)
    pagingState: $previousPagingState,    // For pagination (default: null)
    serialConsistency: SerialConsistency::SERIAL, // Serial consistency (default: null)
    defaultTimestamp: 1640995200000,      // Default timestamp (microseconds, default: null)
    namesForValues: true,                 // Use named parameters (auto-detected if null)
    keyspace: 'my_keyspace',              // Per-request keyspace (v5 only, default: null)
    nowInSeconds: time(),                 // Current time override (v5 only, default: null)
);
```

#### Execute Options
```php
use Cassandra\Request\Options\ExecuteOptions;

$executeOptions = new ExecuteOptions(
    // All QueryOptions properties plus:
    skipMetadata: true,                   // Skip result metadata (default: false)
    autoPrepare: false,                   // Not applicable for execute
    pageSize: 500,
    namesForValues: true,
    // ... other QueryOptions
);
```

#### Prepare Options
```php
use Cassandra\Request\Options\PrepareOptions;

$prepareOptions = new PrepareOptions(
    keyspace: 'my_keyspace',              // Keyspace for preparation (v5 only)
);
```

#### Batch Options
```php
use Cassandra\Request\Options\BatchOptions;
use Cassandra\SerialConsistency;

$batchOptions = new BatchOptions(
    serialConsistency: SerialConsistency::LOCAL_SERIAL,
    defaultTimestamp: 1640995200000,      // Microseconds since epoch
    keyspace: 'my_keyspace',              // v5 only
    nowInSeconds: time(),                 // v5 only
);
```

### Advanced Configuration

#### Value Encoding Configuration
```php
use Cassandra\Value\ValueEncodeConfig;
use Cassandra\Value\EncodeOption\DateEncodeOption;
use Cassandra\Value\EncodeOption\DurationEncodeOption;
use Cassandra\Value\EncodeOption\TimeEncodeOption;
use Cassandra\Value\EncodeOption\TimestampEncodeOption;
use Cassandra\Value\EncodeOption\VarintEncodeOption;

$conn->configureValueEncoding(new ValueEncodeConfig(
    dateEncodeOption: DateEncodeOption::AS_DATETIME_IMMUTABLE,
    durationEncodeOption: DurationEncodeOption::AS_DATEINTERVAL,
    timeEncodeOption: TimeEncodeOption::AS_DATETIME_IMMUTABLE,
    timestampEncodeOption: TimestampEncodeOption::AS_DATETIME_IMMUTABLE,
    varintEncodeOption: VarintEncodeOption::AS_STRING,
));
```

#### Event Listeners
```php
use Cassandra\EventListener;
use Cassandra\WarningsListener;

// Event listener
$conn->addEventListener(new class implements EventListener {
    public function onEvent(\Cassandra\Response\Event $event): void {
        error_log("Cassandra event: " . $event->getType());
    }
});

// Warnings listener
$conn->registerWarningsListener(new class implements WarningsListener {
    public function onWarnings(array $warnings, $request, $response): void {
        foreach ($warnings as $warning) {
            error_log("Cassandra warning: $warning");
        }
    }
});
```

Notes
-----

- `pageSize` is clamped to a minimum of 100 by the client for efficiency.
- If you supply non-`Value\*` PHP values with `QueryOptions(autoPrepare: true)`, the driver auto-prepares + executes for correct typing.
- On `UNPREPARED` server errors, the driver transparently re-prepares and retries the execution.
- Always use fully-qualified table names in `PREPARE` statements.

```php
class SecurityAuditLogger
{
    private string $logFile;
    
    public function __construct(string $logFile = '/var/log/cassandra-audit.log')
    {
        $this->logFile = $logFile;
    }
    
    public function logQuery(string $cql, array $values = [], ?string $user = null): void
    {
        $entry = [
            'timestamp' => date('c'),
            'user' => $user ?? $_SERVER['USER'] ?? 'unknown',
            'query' => $cql,
            'parameter_count' => count($values),
            'client_ip' => $_SERVER['REMOTE_ADDR'] ?? 'unknown'
        ];
        
        file_put_contents($this->logFile, json_encode($entry) . "\n", FILE_APPEND | LOCK_EX);
    }
    
    public function logSecurityEvent(string $event, array $context = []): void
    {
        $entry = [
            'timestamp' => date('c'),
            'event' => $event,
            'context' => $context,
            'client_ip' => $_SERVER['REMOTE_ADDR'] ?? 'unknown'
        ];
        
        file_put_contents($this->logFile, json_encode($entry) . "\n", FILE_APPEND | LOCK_EX);
    }
}
```

Frequently Asked Questions (FAQ)
--------------------------------

### General Questions

**Q: What's the difference between this library and the DataStax PHP Driver?**

A: The main differences are:
- **Pure PHP**: No C extensions required, easier deployment
- **Protocol v5 Support**: Full support for latest Cassandra protocol features
- **Active Development**: Actively maintained with regular updates
- **Modern PHP**: Built for PHP 8.1+ with modern language features

**Q: Can I use this with older versions of Cassandra?**

A: Yes! The library supports protocol versions v3, v4, and v5:
- Cassandra 2.1+: Protocol v3
- Cassandra 2.2+: Protocol v4 (recommended)
- Cassandra 4.0+: Protocol v5 (recommended for new deployments)

### Installation and Setup

**Q: Do I need any PHP extensions?**

A: The library works with standard PHP, but some extensions enhance functionality:
- `ext-sockets`: Required for SocketNodeConfig (alternative: StreamNodeConfig)
- `ext-bcmath` or `ext-gmp`: Improves performance for large integer operations (Varint, Decimal)
- `ext-openssl`: For SSL/TLS connections

**Q: Can I run this on 32-bit PHP?**

A: Yes, but with limitations. Some data types (Bigint, Counter, Duration, Time, Timestamp) may lose precision on 32-bit systems. 64-bit PHP is recommended for full compatibility.

### Data Types and Modeling

**Q: How do I handle complex data structures?**

A: Use collections and UDTs:
```php
// Map
$profile = MapCollection::fromValue(['role' => 'admin', 'level' => 'senior'], Type::VARCHAR, Type::VARCHAR);

// List
$tags = ListCollection::fromValue(['php', 'cassandra', 'database'], Type::VARCHAR);

// UDT
$address = UDT::fromValue(
    ['street' => '123 Main St', 'city' => 'New York', 'zip' => '10001'],
    ['street' => Type::VARCHAR, 'city' => Type::VARCHAR, 'zip' => Type::VARCHAR]
);
```

**Q: How do I work with timestamps?**

A: Use the Timestamp value class:
```php
use Cassandra\Value\Timestamp;

// Current time
$now = Timestamp::now();

// From string
$timestamp = Timestamp::fromValue('2024-01-15T10:30:00Z');

// From Unix timestamp
$timestamp = Timestamp::fromValue(1705312200);
```

Migration Guide
---------------

### From DataStax PHP Driver

If you're migrating from the DataStax PHP Driver, here are the key differences and migration steps:

#### Connection Setup
```php
// DataStax Driver (old)
$cluster = Cassandra::cluster()
    ->withContactPoints('127.0.0.1')
    ->withPort(9042)
    ->withCredentials('username', 'password')
    ->build();
$session = $cluster->connect('keyspace_name');

// php-cassandra (new)
use Cassandra\Connection;
use Cassandra\Connection\SocketNodeConfig;

$conn = new Connection([
    new SocketNodeConfig('127.0.0.1', 9042, 'username', 'password')
], keyspace: 'keyspace_name');
$conn->connect();
```

#### Query Execution
```php
// DataStax Driver (old)
$statement = new Cassandra\SimpleStatement('SELECT * FROM users WHERE id = ?');
$result = $session->execute($statement, ['arguments' => [$userId]]);

// php-cassandra (new)
$result = $conn->query('SELECT * FROM users WHERE id = ?', [$userId])->asRowsResult();
```

#### Prepared Statements
```php
// DataStax Driver (old)
$statement = $session->prepare('SELECT * FROM users WHERE id = ?');
$result = $session->execute($statement, ['arguments' => [$userId]]);

// php-cassandra (new)
$prepared = $conn->prepare('SELECT * FROM users WHERE id = ?');
$result = $conn->execute($prepared, [$userId])->asRowsResult();
```

#### Data Types
```php
// DataStax Driver (old)
$uuid = new Cassandra\Uuid('550e8400-e29b-41d4-a716-446655440000');
$timestamp = new Cassandra\Timestamp(time());

// php-cassandra (new)
use Cassandra\Value\Uuid;
use Cassandra\Value\Timestamp;

$uuid = Uuid::fromValue('550e8400-e29b-41d4-a716-446655440000');
$timestamp = Timestamp::fromValue(time() * 1000);
```

#### Async Operations
```php
// DataStax Driver (old)
$future = $session->executeAsync($statement);
$result = $future->get();

// php-cassandra (new)
$statement = $conn->queryAsync('SELECT * FROM users');
$result = $statement->getRowsResult();
```

### Migration Checklist

- [ ] **Update connection setup** - Replace cluster builder with Connection and NodeConfig
- [ ] **Update query methods** - Replace execute() with query() and asRowsResult()
- [ ] **Update data types** - Replace Cassandra\* types with Cassandra\Value\* types
- [ ] **Update prepared statements** - Use new prepare/execute pattern
- [ ] **Update async operations** - Replace futures with statement handles
- [ ] **Update error handling** - Use new exception hierarchy
- [ ] **Update batch operations** - Use new Batch class
- [ ] **Test thoroughly** - Verify all functionality works as expected

Examples
--------

This library includes comprehensive examples in the [`examples/`](examples/) directory:

- **[basic_usage.php](examples/basic_usage.php)** - Essential operations and getting started
- **[async_operations.php](examples/async_operations.php)** - Asynchronous request handling
- **[data_types.php](examples/data_types.php)** - Working with all Cassandra data types
- **[batch_operations.php](examples/batch_operations.php)** - Batch processing patterns
- **[ssl_connection.php](examples/ssl_connection.php)** - Secure SSL/TLS connections
- **[event_handling.php](examples/event_handling.php)** - Event system usage
- **[object_mapping.php](examples/object_mapping.php)** - Object-oriented result handling
- **[configuration.php](examples/configuration.php)** - Advanced configuration options

See the [examples README](examples/README.md) for detailed information about running and using the examples.

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

Tracing notes
-------------

- Use tracing sparingly in production; it adds overhead.
- Read the trace id from the result to correlate with server logs (if enabled).

Performance tips
----------------

- Prefer prepared statements for hot paths; the driver caches prepared metadata.
- Iterate results instead of materializing large arrays.

Version support
---------------

- Cassandra 4.x/5.x tested. Protocols v3/v4/v5 supported; features like per-request keyspace/now_in_seconds require v5.


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


Contributing
------------

Contributions are welcome! Here's how to get started:

### Development Setup

1. **Fork and Clone**
   ```bash
   git clone https://github.com/your-username/php-cassandra.git
   cd php-cassandra
   ```

2. **Install Dependencies**
   ```bash
   composer install
   ```

3. **Start Development Environment**
   ```bash
   docker compose up -d
   composer test:integration:init
   ```

4. **Run Tests**
   ```bash
   composer test:unit
   composer test:integration:run
   ```

### Contribution Guidelines

#### Code Standards
- **PHP 8.1+**: Use modern PHP features and syntax
- **PSR-12**: Follow PHP-FIG coding standards
- **Type Hints**: Use strict typing everywhere possible
- **Documentation**: Document all public methods and classes
- **Tests**: Include tests for all new functionality

### Contributors

- **Michael Roosz** - Current maintainer and lead developer
- **Shen Zhenyu** - Original driver development
- **Evseev Nikolay** - Foundation and early development

Special thanks to all contributors who have helped make this library better.

### Supporting the Project

If you find this library useful, consider:

- ⭐ **Starring the repository** on GitHub
- 🐛 **Reporting bugs** and suggesting features
- 📝 **Contributing code** or documentation
- 💬 **Sharing your experience** with the community
- 📚 **Writing tutorials** or blog posts

Your support helps keep this project active and improving!
