php-cassandra: A modern Cassandra client for PHP
================================================

[![Latest Stable Version](https://poser.pugx.org/mroosz/php-cassandra/v/stable)](https://packagist.org/packages/mroosz/php-cassandra)
[![License](https://poser.pugx.org/mroosz/php-cassandra/license)](https://packagist.org/packages/mroosz/php-cassandra)
[![PHP Version Require](https://poser.pugx.org/mroosz/php-cassandra/require/php)](https://packagist.org/packages/mroosz/php-cassandra)
[![Total Downloads](https://poser.pugx.org/mroosz/php-cassandra/downloads)](https://packagist.org/packages/mroosz/php-cassandra)

[![Static Analysis: PHPStan](https://img.shields.io/badge/Static%20analysis-PHPStan-1e90ff?logo=phpstan&logoColor=white)](https://phpstan.org)
[![Static Analysis: Psalm](https://img.shields.io/badge/Static%20analysis-Psalm-4A41BE?logo=psalm&logoColor=white)](https://psalm.dev)
[![Tests: PHPUnit](https://img.shields.io/badge/Tests-PHPUnit-6C78AF?logo=phpunit&logoColor=white)](https://phpunit.de)

php-cassandra is a pure-PHP client for Apache Cassandra and ScyllaDB with support for CQL binary protocol v3, v4 and v5 (Cassandra 2.1+ incl. 3.x–5.x; ScyllaDB 6.2, 2025.x, 2026.x), synchronous and asynchronous APIs, prepared statements, batches, result iterators, object mapping, SSL/TLS, and LZ4 compression.

**Packagist:** [mroosz/php-cassandra](https://packagist.org/packages/mroosz/php-cassandra)  
**Repository:** [GitHub – MichaelRoosz/php-cassandra](https://github.com/MichaelRoosz/php-cassandra)

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
  - [Collection updates](#collection-updates)
  - [Lightweight transactions (LWT)](#lightweight-transactions-lwt)
  - [JSON support](#json-support)
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
  - [Connection tuning examples](#connection-tuning-examples)
  - [Configuring value encoding](#configuring-value-encoding)
  - [Warnings listener](#warnings-listener)
  - [Event processing patterns](#event-processing-patterns)
  - [v5 keyspace per request](#v5-keyspace-per-request)
  - [Tracing notes](#tracing-notes)
  - [Performance tips](#performance-tips)
  - [Benchmarks](#benchmarks)
  - [Version support](#version-support)
    - [Server compatibility and required settings](#server-compatibility-and-required-settings)
      - [Server-side features that are off by default](#server-side-features-that-are-off-by-default)
  - [API reference (essentials)](#api-reference-essentials)
  - [Changelog](#changelog)
  - [License](#license)
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
- **CQL Coverage**: Incremental collection updates, lightweight transactions, JSON, TTL/timestamps
- **Results**: Iterators, multiple fetch styles, object mapping
- **Events**: Schema/status/topology change notifications
- **Advanced**: LZ4 compression, server overload signaling, tracing support


Requirements
------------

### System Requirements

| Component | Minimum | Recommended | Notes |
|-----------|---------|-------------|-------|
| **PHP Version** | 8.1.0 | 8.3+ | Latest stable version recommended |
| **Architecture** | 32-bit/64-bit | 64-bit | 64-bit required for Bigint/Counter/Date/Duration/Time/Timestamp types and defaultTimestamp request option (unsupported on 32-bit) |

### PHP Extensions

No PHP extension is required; the library runs on a minimal PHP build. The following are optional and only enhance functionality:

| Extension | Required | Purpose | Notes |
|-----------|----------|---------|-------|
| **sockets** | Optional | Socket transport | Required for connections configured with `SocketNodeConfig`; `StreamNodeConfig` connections need no extension |
| **openssl** | Optional | TLS/SSL encrypted connections | Required for `tls://` connections configured with `StreamNodeConfig` |
| **lz4** | Optional | Native LZ4 (de)compression | Used automatically when present for much faster compressed connections; a pure-PHP implementation is the transparent fallback |
| **gmp** or **bcmath** | Optional | Faster large integer math | Speeds up the `Varint` and `Decimal` types; `gmp` is preferred when both are present, and a pure-PHP calculator is the fallback |

### Data Type Compatibility

Some Cassandra data types require 64-bit PHP and are unsupported on 32-bit:

| Type | 32-bit PHP | 64-bit PHP | Notes |
|------|------------|------------|-------|
| `Bigint` | ⚠️ Partial | ✅ Full | Supported if the value is within 32-bit range |
| `Counter` | ⚠️ Partial | ✅ Full | Supported if the value is within 32-bit range |
| `Date` | ❌ Unsupported | ✅ Full | Requires 64-bit PHP |
| `Duration` | ❌ Unsupported | ✅ Full | Requires 64-bit PHP |
| `Time` | ❌ Unsupported | ✅ Full | Requires 64-bit PHP |
| `Timestamp` | ❌ Unsupported | ✅ Full | Requires 64-bit PHP |

Additionally, the `defaultTimestamp` request option (in `QueryOptions` and `BatchOptions`) requires 64-bit PHP and is unsupported on 32-bit.

Installation
------------

### Using Composer (Recommended)

```bash
composer require mroosz/php-cassandra
```

Then include Composer's autoloader in your application entrypoint (if not already):

```php
<?php
require __DIR__ . '/vendor/autoload.php';
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

```php
<?php
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Value\Uuid;
use Cassandra\Consistency;

// Prepare a statement
$prepared = $conn->prepare('SELECT * FROM users WHERE id = ? AND status = ?');

// Execute with positional parameters
$result = $conn->execute(
    $prepared, 
    [
        Uuid::fromValue('550e8400-e29b-41d4-a716-446655440000'),
        'active'
    ],
    consistency: Consistency::LOCAL_QUORUM,
    options: new ExecuteOptions(pageSize: 100)
)->asRowsResult();

foreach ($result as $user) {
    echo "User: {$user['name']} ({$user['email']})\n";
}

// Execute with named parameters
$namedPrepared = $conn->prepare('SELECT * FROM users WHERE email = :email AND org_id = :org_id');
$result = $conn->execute(
    $namedPrepared,
    ['email' => 'john@example.com', 'org_id' => 123],
    options: new ExecuteOptions(namesForValues: true)
)->asRowsResult();
```

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
use Cassandra\Exception\ServerException;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\CassandraException;

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
    // Inspect $e->getContext() and $e->getErrorContext() for server-provided details
    
} catch (ConnectionException $e) {
    echo "Connection error: " . $e->getMessage() . "\n";
    
} catch (CassandraException $e) {
    echo "Client error: " . $e->getMessage() . "\n";
}
```

### SSL/TLS Connection Example

```php
<?php
use Cassandra\Connection\StreamNodeConfig;

// Secure connection with TLS. A non-empty sslOptions array enables TLS
// automatically (equivalent to prefixing the host with "tls://").
$secureNode = new StreamNodeConfig(
    host: 'cassandra.example.com',
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

// Stream transport (plaintext)
$streamNode = new StreamNodeConfig(
    host: 'cassandra.example.com',
    port: 9042,
    username: 'user',
    password: 'secret',
    connectTimeoutInSeconds: 5,
    timeoutInSeconds: 15,
);

// Stream transport with SSL/TLS - a non-empty sslOptions array enables TLS
// automatically; alternatively prefix the host with "tls://"
$streamTlsNode = new StreamNodeConfig(
    host: 'cassandra.example.com',
    port: 9042,
    username: 'user',
    password: 'secret',
    connectTimeoutInSeconds: 5,
    timeoutInSeconds: 15,
    sslOptions: [
        // See [PHP SSL context options](https://www.php.net/manual/en/context.ssl.php)
        'cafile' => '/etc/ssl/certs/ca.pem',
        'verify_peer' => true,
        'verify_peer_name' => true,
    ]
);

// Socket transport
$socketNode = new SocketNodeConfig(
    host: '10.0.0.10',
    port: 9042,
    username: 'user',
    password: 'secret',
    // See [PHP socket_get_option documentation](https://www.php.net/manual/en/function.socket-get-option.php)
    socketOptions: [SO_RCVTIMEO => ['sec' => 15, 'usec' => 0]],
    connectTimeoutInSeconds: 5
);

$conn = new Connection([$streamNode, $streamTlsNode, $socketNode], keyspace: 'app');
$conn->connect();
```

Connection options are provided via `ConnectionOptions`:
- `enableCompression` = use LZ4 if enabled on server
- `throwOnOverload` = true to ask server to throw on overload (v4+)
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
- `defaultTimestamp` (microseconds since epoch)
  - Requires 64-bit PHP; unsupported on 32-bit
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
- `BatchOptions`: `serialConsistency`, `defaultTimestamp` (64-bit PHP only), `keyspace` (v5), `nowInSeconds` (v5).

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

**Advanced fetching examples:**

```php
// Fetch single row
$result = $conn->query('SELECT id, name, email FROM users WHERE id = ?', [$userId])->asRowsResult();
$user = $result->fetch(FetchType::ASSOC);
if ($user) {
    echo "User: {$user['name']} <{$user['email']}>\n";
}

// Fetch all rows at once
$allUsers = $result->fetchAll(FetchType::ASSOC);
foreach ($allUsers as $user) {
    echo "User: {$user['name']}\n";
}

// Fetch specific column values
$result = $conn->query('SELECT name FROM users WHERE org_id = ?', [123])->asRowsResult();
$names = $result->fetchAllColumns(0); // Get all values from first column
print_r($names);

// Fetch key-value pairs
$result = $conn->query('SELECT id, name FROM users WHERE active = true')->asRowsResult();
$userMap = $result->fetchAllKeyPairs(0, 1); // id => name mapping
print_r($userMap);

// Different fetch types
$result = $conn->query('SELECT id, name, email FROM users LIMIT 5')->asRowsResult();

// Associative array (default)
$row = $result->fetch(FetchType::ASSOC);
// Returns: ['id' => '...', 'name' => '...', 'email' => '...']

// Numeric array
$row = $result->fetch(FetchType::NUM);
// Returns: [0 => '...', 1 => '...', 2 => '...']

// Both associative and numeric
$row = $result->fetch(FetchType::BOTH);
// Returns: ['id' => '...', 0 => '...', 'name' => '...', 1 => '...', ...]
```

**Pagination example:**

```php
use Cassandra\Request\Options\QueryOptions;

$pageSize = 100;
$options = new QueryOptions(pageSize: $pageSize);
$result = $conn->query('SELECT * FROM large_table', [], options: $options)->asRowsResult();

$totalProcessed = 0;
do {
    foreach ($result as $row) {
        // Process each row
        echo "Processing: {$row['id']}\n";
        $totalProcessed++;
    }
    
    $pagingState = $result->getRowsMetadata()->pagingState;
    if ($pagingState === null) {
        break; // No more pages
    }
    
    // Fetch next page
    $options = new QueryOptions(pageSize: $pageSize, pagingState: $pagingState);
    $result = $conn->query('SELECT * FROM large_table', [], options: $options)->asRowsResult();
    
} while (true);

echo "Total processed: {$totalProcessed} rows\n";
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
- A PHP scalar/array matching the type; the driver will convert it when metadata is available (prepared statements, or `query()` with the default `autoPrepare`).

Without that metadata (e.g. `query(..., autoPrepare: false)` or `Batch::appendQuery()` simple-query values) a bare PHP `int` is encoded as 32-bit `int`, and a bare `DateTime` has no unambiguous encoding — wrap large integers in `Value\Bigint` and temporal values in `Value\Timestamp`/`Date`/`Time` (or use a prepared statement). The driver throws rather than silently sending a wrong value.

Examples:
```php
use Cassandra\Value\Ascii;
use Cassandra\Value\Bigint;
use Cassandra\Value\Blob;
use Cassandra\Value\Boolean;
use Cassandra\Value\Counter;
use Cassandra\Value\Custom;
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
Blob::fromValue("\x01\x02");
Boolean::fromValue(true);
Counter::fromValue(1000);
Custom::fromValue('custom_data', 'my.custom.Type');
Decimal::fromValue('123.456');
Double::fromValue(2.718281828459);
Float32::fromValue(2.718);
Inet::fromValue('192.168.0.1');
Int32::fromValue(-123);
Smallint::fromValue(2048);
Timeuuid::fromValue('8db96410-8dba-11f0-b0eb-325096b39f47');
Tinyint::fromValue(12);
Uuid::fromValue('78b58041-06dd-4181-a14f-ce0c1979f51c');
Varchar::fromValue('hello ✅');
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

### UUID / Timeuuid input forms

`Cassandra\Value\Uuid` and `Cassandra\Value\Timeuuid` accept any of three forms, distinguished by length:

- the canonical 36-character string `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` (case-insensitive);
- the compact 32-character undashed hex string `xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`;
- the raw 16-byte binary form (for example a value read with `UuidEncodeOption::AS_BINARY`, which can be re-bound directly without a format→parse round-trip).

The value is stored raw internally, so `getBinary()` is a no-op and `getValue()` always returns the canonical lowercase string. Any string that is none of these forms is rejected with a `Cassandra\Exception\ValueException`.

```php
use Cassandra\Value\Uuid;

Uuid::fromValue('550e8400-e29b-41d4-a716-446655440000'); // canonical
Uuid::fromValue('550e8400e29b41d4a716446655440000');     // undashed hex
Uuid::fromValue($rawSixteenBytes);                       // raw binary
```

Type definition syntax for complex values
----------------------------------------

For complex types, the driver needs a type definition to encode PHP values. Wherever you see a parameter like `\Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>)`, you can either pass a scalar `Type::...` (for simple elements) or a definition array with nested types for complex structures. The common shapes are:

- List: `['type' => Type::LIST, 'valueType' => <elementType>, 'isFrozen' => bool]`
- Set: `['type' => Type::SET, 'valueType' => <elementType>, 'isFrozen' => bool]`
- Map: `['type' => Type::MAP, 'keyType' => <keyType>, 'valueType' => <valueType>, 'isFrozen' => bool]`
- Tuple: `['type' => Type::TUPLE, 'valueTypes' => [<t1>, <t2>, ...]]`
- UDT: `['type' => Type::UDT, 'valueTypes' => ['field' => <type>, ...], 'isFrozen' => bool, 'keyspace' => 'ks', 'name' => 'udt_name']`
- Vector: `['type' => Type::VECTOR, 'valueType' => <elementType>, 'dimensions' => int]`

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

// Vector<float> with 3 dimensions
Vector::fromValue([0.1, -0.5, 0.8], Type::FLOAT, dimensions: 3);

// Vector<double> with 128 dimensions (common for embeddings)
Vector::fromValue(array_fill(0, 128, 0.0), Type::DOUBLE, dimensions: 128);
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

Collection updates
------------------

On **non-frozen** `set`, `list`, and `map` columns you can add or remove individual elements instead of rewriting the whole collection. This is plain CQL ([DataStax docs](https://docs.datastax.com/en/cql-oss/3.3/cql/cql_using/useInsertMap.html)) — php-cassandra adds no special API: you write the `UPDATE` yourself and bind only the **delta** (the elements to add or remove) as `?`.

| Operation | `set` | `list` | `map` |
|---|---|---|---|
| Replace whole collection | `INSERT`, or `SET col = ?` | `INSERT`, or `SET col = ?` | `INSERT`, or `SET col = ?` |
| Add | `SET col = col + ?` (merges members) | `SET col = col + ?` (**append**)<br>`SET col = ? + col` (**prepend**) | `SET col = col + ?` (merges entries) |
| Remove | `SET col = col - ?` (removes those members) | `SET col = col - ?` (removes **by value** — every occurrence) | `SET col = col - ?` (removes **keys**; pass a set/list of keys) |
| Single element | – | `SET col[?] = ?` (by index)<br>`DELETE col[?] FROM …` (by index) | `SET col[?] = ?` (by key)<br>`DELETE col[?] FROM …` (by key) |
| Clear | `SET col = {}` or `DELETE col FROM …` | `SET col = ?` with `[]`, or `DELETE col FROM …` | `SET col = {}` or `DELETE col FROM …` |

Notes:

- Use `?` for the whole collection operand in prepared queries. `{ ? }` / `[ ? ]` inside the query string is **not** valid CQL (curly braces and square brackets are literal syntax only). Indexes and keys in `col[?]` *are* bindable.
- List `-` matches **by value**, not by position, and removes every occurrence. To remove by position use `DELETE col[i] FROM …`, which requires an internal read and is unsafe under concurrent writes — prefer removal by value.
- Assigning `null` to a map key (`SET col[?] = ?` with `null`) deletes that entry.
- An **empty** collection is stored as `null`; reading it back yields `null`, not `[]`.
- **Frozen** collections (`frozen<set<...>>`, etc.) cannot use `+`/`-` — the server rejects it with an `InvalidException`. Assign the full value instead.
- Incremental updates combine with `USING TTL` and with conditions (`IF …`). Because a non-frozen collection is multi-cell, `TTL(col)` returns one entry **per element**.
- With default `autoPrepare`, plain PHP arrays work as binds (the driver prepares the statement to learn the types — see [Data types](#data-types)); passing an explicit `SetCollection` / `MapCollection` / `ListCollection` value skips that step and always works.

Each example below is shown twice: first with explicit `Value\...::fromValue()` objects (always works, no prepare step), then with plain PHP values (relies on the default `autoPrepare`, which prepares the statement to learn the column types).

**Set** — add and remove members:

```php
use Cassandra\Type;
use Cassandra\Value\Int32;
use Cassandra\Value\SetCollection;

// --- With ::fromValue() ---
$conn->query(
    'INSERT INTO ks.users (id, tags) VALUES (?, ?)',
    [Int32::fromValue(1), SetCollection::fromValue(['php'], Type::VARCHAR)]
);

$conn->query(
    'UPDATE ks.users SET tags = tags + ? WHERE id = ?',
    [SetCollection::fromValue(['cassandra'], Type::VARCHAR), Int32::fromValue(1)]
);

$conn->query(
    'UPDATE ks.users SET tags = tags - ? WHERE id = ?',
    [SetCollection::fromValue(['php'], Type::VARCHAR), Int32::fromValue(1)]
);

// --- With plain PHP values (autoPrepare) ---
$conn->query('INSERT INTO ks.users (id, tags) VALUES (?, ?)', [1, ['php']]);
$conn->query('UPDATE ks.users SET tags = tags + ? WHERE id = ?', [['cassandra'], 1]);
$conn->query('UPDATE ks.users SET tags = tags - ? WHERE id = ?', [['php'], 1]);

// Clear the set (both store null)
$conn->query('UPDATE ks.users SET tags = {} WHERE id = ?', [1]);
$conn->query('DELETE tags FROM ks.users WHERE id = ?', [1]);
```

**Map** — merge entries, remove keys, or address a single key:

```php
use Cassandra\Type;
use Cassandra\Value\MapCollection;
use Cassandra\Value\SetCollection;
use Cassandra\Value\Uuid;

// --- With ::fromValue() ---
$id = Uuid::fromValue('5b6962dd-3f90-4c93-8f61-eabfa4a803e2');

$conn->query(
    'UPDATE ks.cyclist_teams SET teams = teams + ? WHERE id = ?',
    [MapCollection::fromValue([2009 => 'DSB Bank'], Type::INT, Type::VARCHAR), $id]
);

// Subtraction takes a set of keys
$conn->query(
    'UPDATE ks.cyclist_teams SET teams = teams - ? WHERE id = ?',
    [SetCollection::fromValue([2013, 2014], Type::INT), $id]
);

// Set one key ...
$conn->query(
    'UPDATE ks.cyclist_teams SET teams[?] = ? WHERE id = ?',
    [2006, 'Team DSB - Ballast Nedam', $id]
);

// ... assign null to delete it ...
$conn->query('UPDATE ks.cyclist_teams SET teams[?] = ? WHERE id = ?', [2006, null, $id]);

// ... or delete it directly
$conn->query('DELETE teams[?] FROM ks.cyclist_teams WHERE id = ?', [2009, $id]);

// --- With plain PHP values (autoPrepare) ---
$id = '5b6962dd-3f90-4c93-8f61-eabfa4a803e2';

$conn->query('UPDATE ks.cyclist_teams SET teams = teams + ? WHERE id = ?', [[2009 => 'DSB Bank'], $id]);
$conn->query('UPDATE ks.cyclist_teams SET teams = teams - ? WHERE id = ?', [[2013, 2014], $id]);
$conn->query('UPDATE ks.cyclist_teams SET teams[?] = ? WHERE id = ?', [2006, 'Team DSB - Ballast Nedam', $id]);
$conn->query('DELETE teams[?] FROM ks.cyclist_teams WHERE id = ?', [2009, $id]);
```

**List** — append, prepend, address a position, or remove by value:

```php
use Cassandra\Type;
use Cassandra\Value\Int32;
use Cassandra\Value\ListCollection;

// --- With ::fromValue() ---
$id = Int32::fromValue(1);

// Append
$conn->query(
    'UPDATE ks.users SET phones = phones + ? WHERE id = ?',
    [ListCollection::fromValue(['555-0100'], Type::VARCHAR), $id]
);

// Prepend
$conn->query(
    'UPDATE ks.users SET phones = ? + phones WHERE id = ?',
    [ListCollection::fromValue(['555-0001'], Type::VARCHAR), $id]
);

// Remove by value (every occurrence)
$conn->query(
    'UPDATE ks.users SET phones = phones - ? WHERE id = ?',
    [ListCollection::fromValue(['555-0100'], Type::VARCHAR), $id]
);

// Overwrite / remove by position (needs an internal read — prefer the two calls above)
$conn->query('UPDATE ks.users SET phones[?] = ? WHERE id = ?', [0, '555-0999', $id]);
$conn->query('DELETE phones[?] FROM ks.users WHERE id = ?', [0, $id]);

// --- With plain PHP values (autoPrepare) ---
$id = 1;

$conn->query('UPDATE ks.users SET phones = phones + ? WHERE id = ?', [['555-0100'], $id]); // append
$conn->query('UPDATE ks.users SET phones = ? + phones WHERE id = ?', [['555-0001'], $id]); // prepend
$conn->query('UPDATE ks.users SET phones = phones - ? WHERE id = ?', [['555-0100'], $id]); // remove by value
$conn->query('UPDATE ks.users SET phones[?] = ? WHERE id = ?', [0, '555-0999', $id]);
$conn->query('DELETE phones[?] FROM ks.users WHERE id = ?', [0, $id]);
```

**Nested collections** (`map<int, frozen<list<text>>>`, `set<frozen<list<int>>>`, `list<frozen<udt>>`) support the same `+`/`-` operations on the outer collection; the frozen inner value is always replaced as a whole.

**Counter** columns use the same `+` pattern with `Counter::fromValue()`:

```php
use Cassandra\Value\Counter;

// With ::fromValue()
$conn->query('UPDATE ks.stats SET value = value + ? WHERE id = ?', [Counter::fromValue(5), 1]);

// With a plain PHP value (autoPrepare)
$conn->query('UPDATE ks.stats SET value = value + ? WHERE id = ?', [5, 1]);
```

Special values:
- `new \Cassandra\Value\NotSet()` encodes a bind variable as NOT SET (distinct from NULL)

Lightweight transactions (LWT)
------------------------------

`INSERT`, `UPDATE` and `DELETE` accept an `IF` clause for compare-and-set semantics (see the [DataStax docs](https://docs.datastax.com/en/cql-oss/3.3/cql/cql_using/useInsertLWT.html)). No special API is needed — a conditional statement simply returns a **rows result** whose first column is `[applied]`:

```php
$result = $conn->query(
    'INSERT INTO ks.cyclists (id, lastname, firstname) VALUES (?, ?, ?) IF NOT EXISTS',
    [1, 'KNETEMANN', 'Roxxane']
)->asRowsResult();

$row = $result->fetch();

if ($row['[applied]'] === true) {
    // the row was created; [applied] is the only column returned
} else {
    // not applied - the conflicting row is returned alongside [applied]
    echo $row['lastname'];
}
```

The same applies to conditional updates and deletes:

```php
// Only update when the current value matches
$conn->query(
    'UPDATE ks.cyclists SET firstname = ? WHERE id = ? IF firstname = ?',
    ['Roxane', 1, 'Roxxane']
);

// Non-equal operators are supported: <, <=, >, >=, != and IN
$conn->query('UPDATE ks.cyclists SET firstname = ? WHERE id = ? IF age > ?', ['Roxane', 1, 20]);
$conn->query('UPDATE ks.cyclists SET firstname = ? WHERE id = ? IF lastname IN ?', ['Roxane', 1, ['VOS', 'BRAND']]);

// Guard against a missing / existing row
$conn->query('UPDATE ks.cyclists SET firstname = ? WHERE id = ? IF EXISTS', ['Roxane', 1]);
$conn->query('DELETE FROM ks.cyclists WHERE id = ? IF EXISTS', [1]);

// Conditions on collections
$conn->query('UPDATE ks.users SET tags = tags + ? WHERE id = ? IF tags CONTAINS ?', [['go'], 1, 'php']);
```

Use `serialConsistency` to choose the Paxos consistency level, and read back with `Consistency::SERIAL` / `Consistency::LOCAL_SERIAL` to observe in-progress transactions:

```php
use Cassandra\Consistency;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\SerialConsistency;

$conn->query(
    'UPDATE ks.cyclists SET firstname = ? WHERE id = ? IF lastname = ?',
    ['Roxane', 1, 'KNETEMANN'],
    Consistency::ONE,
    new QueryOptions(serialConsistency: SerialConsistency::LOCAL_SERIAL)
);

$conn->query('SELECT * FROM ks.cyclists WHERE id = ?', [1], Consistency::SERIAL);
```

Conditional statements also work inside a batch (all statements must target the same partition):

```php
use Cassandra\Consistency;
use Cassandra\Request\BatchType;

$batch = $conn->createBatchRequest(BatchType::LOGGED, Consistency::ONE);
$batch->appendQuery('INSERT INTO ks.cyclists (id, lastname) VALUES (?, ?) IF NOT EXISTS', [2, 'VOS']);

$applied = $conn->batch($batch)->asRowsResult()->fetch()['[applied]'];
```

Notes:

- LWT costs roughly four extra round-trips compared to a normal write — use it only where you need it.
- `USING TIMESTAMP` is not allowed together with `IF NOT EXISTS`; the timestamp comes from the transaction itself.

JSON support
------------

CQL can read and write rows as JSON documents (see the [DataStax docs](https://docs.datastax.com/en/cql-oss/3.3/cql/cql_using/useInsertJSON.html)). The driver transports the document as a plain `varchar`, so no dedicated API is required:

```php
// Insert a whole row from a JSON document (note: no VALUES keyword, no column list)
$conn->query(
    'INSERT INTO ks.cyclist_category JSON ?',
    [json_encode(['id' => 1, 'lastname' => 'SUTHERLAND', 'category' => 'GC', 'points' => 780])]
);

// Read a row back as JSON: a single column named [json]
$row = $conn->query('SELECT JSON * FROM ks.cyclist_category WHERE id = ?', [1])
    ->asRowsResult()
    ->fetch();

$data = json_decode($row['[json]'], true);

// fromJson() / toJson() work on individual columns
$conn->query('INSERT INTO ks.cyclist_category (id, tags) VALUES (?, fromJson(?))', [2, '["a","b"]']);
$conn->query('SELECT toJson(tags) AS tags_json FROM ks.cyclist_category WHERE id = ?', [2]);
```

By default a column that is absent from the document is written as `null`. Append `DEFAULT UNSET` to leave such columns untouched instead:

```php
$conn->query('INSERT INTO ks.cyclist_category JSON ? DEFAULT UNSET', ['{"id": 1, "points": 900}']);
$conn->query('INSERT INTO ks.cyclist_category JSON ? DEFAULT NULL', ['{"id": 1, "points": 900}']);
```

Events
------

Register a listener and subscribe for events on the connection:
```php
use Cassandra\EventListener;
use Cassandra\Response\Event;
use Cassandra\Request\Register;
use Cassandra\EventType;

$conn->registerEventListener(new class () implements EventListener {
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
    // Blocks until an event arrives. An idle event stream is not an error, so
    // this keeps waiting across transport read timeouts; meanwhile an OPTIONS
    // heartbeat is sent whenever the connection goes quiet, so a connection
    // that died is still noticed. Pass a timeout to get null instead of
    // blocking forever:
    //     $event = $conn->waitForNextEvent(timeoutInSeconds: 60.0);
    $event = $conn->waitForNextEvent();
}
```

Non-blocking event polling:
```php
// In your app loop, poll without blocking
if ($event = $conn->tryReadNextEvent()) {
    // handle $event
}

// Or drain all currently available events
while ($event = $conn->tryReadNextEvent()) {
    // handle $event
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

The async API lets you pipeline multiple requests without blocking. Each async method returns a `Cassandra\Statement` handle that you can resolve later.

You now have both blocking and non-blocking control:

- Blocking per statement: `getResult()` / `getRowsResult()` / `waitForResponse()`
- Blocking for sets: `waitForStatements(array $statements)` and `waitForAllPendingStatements()`
- Non-blocking/polling:
  - `drainAvailableResponses(int $max = PHP_INT_MAX): int` — processes up to `max` responses if available
  - `tryResolveStatement(Statement $statement): bool` — resolves a specific statement if possible
  - `tryResolveStatements(array $statements, int $max = PHP_INT_MAX): int` — resolves from a set without blocking
  - `waitForAnyStatement(array $statements): Statement` — blocks until any of the given statements completes

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

Waiting for all responses:
```php
// Issue several statements
$handles = [];
for ($i = 0; $i < 10; $i++) {
    $handles[] = $conn->queryAsync('SELECT now() FROM system.local');
}

$conn->waitForStatements($handles);

foreach ($handles as $h) {
    $rows = $h->getRowsResult();
    // process
}
```

Non-blocking draining and polling:
```php
// Fire off work in various places...

// Later in your loop: non-blocking drain up to 32 available responses
$processed = $conn->drainAvailableResponses(32);
if ($processed > 0) {
    // some statements just became ready; you can consume their results now
}

// Or: non-blocking check for a specific statement
if ($conn->tryResolveStatement($s1)) {
    $rows = $s1->getRowsResult();
}

// Or: wait until any of several statements completes
$ready = $conn->waitForAnyStatement([$s1, $s2]);
// $ready is whichever completed first
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

Advanced waiting:
```php
// Block until any statement completes (null if the wait bound elapses first):
$stmt = $conn->waitForAnyStatement([$s1, $s2, $s3], timeoutInSeconds: 5.0);

// Block until the next event arrives (null once the timeout elapses):
$event = $conn->waitForNextEvent(timeoutInSeconds: 30.0);
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

Notes:
- Compression is negotiated during STARTUP. When enabled, the client accepts server-compressed frames and transparently decompresses them.
- The client may still send some frames uncompressed depending on size/heuristics; this is allowed by the protocol.
- LZ4 works out of the box with a pure-PHP implementation. If the native [`lz4` PHP extension](https://github.com/kjdev/php-ext-lz4) is installed it is detected and used automatically for substantially faster (de)compression — no configuration or code change required.

Error handling
--------------

php-cassandra provides comprehensive error handling with a well-structured exception hierarchy. Understanding these exceptions helps you build robust applications with proper error recovery.

### Exception Hierarchy

```
\Exception
└── \Cassandra\Exception\CassandraException (base exception)
    ├── \Cassandra\Exception\CompressionException (compression errors)
    ├── \Cassandra\Exception\ConnectionException (connection/transport errors)
    ├── \Cassandra\Exception\NodeException (node I/O errors)
    │   ├── \Cassandra\Exception\SocketException
    │   └── \Cassandra\Exception\StreamException
    ├── \Cassandra\Exception\RequestException (request errors)
    ├── \Cassandra\Exception\RequestTimeoutException (server did not answer in time)
    ├── \Cassandra\Exception\ResponseException (response errors)
    ├── \Cassandra\Exception\ServerException (server-side errors)
    │   ├── \Cassandra\Exception\ServerException\AlreadyExistsException
    │   ├── \Cassandra\Exception\ServerException\AuthenticationErrorException
    │   ├── \Cassandra\Exception\ServerException\CasWriteUnknownException
    │   ├── \Cassandra\Exception\ServerException\CdcWriteFailureException
    │   ├── \Cassandra\Exception\ServerException\ConfigErrorException
    │   ├── \Cassandra\Exception\ServerException\FunctionFailureException
    │   ├── \Cassandra\Exception\ServerException\InvalidException
    │   ├── \Cassandra\Exception\ServerException\IsBootstrappingException
    │   ├── \Cassandra\Exception\ServerException\OverloadedException
    │   ├── \Cassandra\Exception\ServerException\ProtocolErrorException
    │   ├── \Cassandra\Exception\ServerException\ReadFailureException
    │   ├── \Cassandra\Exception\ServerException\ReadTimeoutException
    │   ├── \Cassandra\Exception\ServerException\ServerErrorException
    │   ├── \Cassandra\Exception\ServerException\SyntaxErrorException
    │   ├── \Cassandra\Exception\ServerException\TruncateErrorException
    │   ├── \Cassandra\Exception\ServerException\UnauthorizedException
    │   ├── \Cassandra\Exception\ServerException\UnavailableException
    │   ├── \Cassandra\Exception\ServerException\UnpreparedException
    │   ├── \Cassandra\Exception\ServerException\WriteFailureException
    │   ├── \Cassandra\Exception\ServerException\WriteTimeoutException
    ├── \Cassandra\Exception\StatementException (result type mismatches)
    ├── \Cassandra\Exception\StringMathException (string math backends)
    ├── \Cassandra\Exception\TypeInfoException (type info building errors)
    ├── \Cassandra\Exception\TypeNameParserException (type parsing errors)
    ├── \Cassandra\Exception\ValueException (invalid value inputs)
    ├── \Cassandra\Exception\ValueFactoryException (value factory/type def errors)
    └── \Cassandra\Exception\VIntCodecException (variable integer codec errors)
```

#### Which timeout is which

Three different exceptions report a timeout, and they mean quite different things:

| Exception | Raised by | Meaning | Node health |
|---|---|---|---|
| `ServerException\ReadTimeoutException` / `WriteTimeoutException` | the coordinator | the server hit *its own* `read_request_timeout` / `write_request_timeout` and said so | fine — it answered |
| `RequestTimeoutException` | the client | the server never answered within `requestTimeoutInSeconds` | not blamed; the connection stays open and only the request that ran out is finished |
| `SocketException` / `StreamException` (with a `SOCKET_TIMEOUT_DURING_*` / `STREAM_TIMEOUT_DURING_*` code) | the transport | the connection made no progress within the stall timeout | suspect — counted as a node failure |

A `ConnectionException` naming the heartbeat is the fourth possibility: the connection went quiet and did not answer its `OPTIONS` probe, so it is treated as dead even though a request may still be outstanding.

### Error Handling Patterns

#### Basic Error Handling
```php
use Cassandra\Exception\StatementException;
use Cassandra\Exception\ServerException;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\RequestTimeoutException;
use Cassandra\Exception\CassandraException;

try {
    $result = $conn->query('SELECT * FROM users WHERE id = ?', [$userId])
        ->asRowsResult();
    
    foreach ($result as $row) {
        // Process row
    }
    
} catch (ServerException $e) {
    // Server returned an error response
    error_log("Server error: " . $e->getMessage());
    
} catch (RequestTimeoutException $e) {
    // The server did not answer within the client-side request timeout.
    // Nothing is known to be wrong with the node — either give the operation a
    // larger budget, or retry it if it is safe to run twice.
    error_log("Request timed out: " . $e->getMessage());
    
} catch (ConnectionException $e) {
    // Network/connection issues
    error_log("Connection error: " . $e->getMessage());
    
} catch (StatementException $e) {
    // Wrong result type access (e.g., calling asRowsResult() on non-rows result)
    error_log("Statement error: " . $e->getMessage());
    
} catch (CassandraException $e) {
    // Other client-side errors
    error_log("Client error: " . $e->getMessage());
}
```

#### Specific Server Error Handling
```php
use Cassandra\Exception\ServerException\{
    UnavailableException,
    ReadTimeoutException,
    WriteTimeoutException,
    OverloadedException,
    SyntaxErrorException,
    InvalidException,
    UnauthorizedException,
    AlreadyExistsException
};

try {
    $conn->query('CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)');
    
} catch (AlreadyExistsException $e) {
    // Table already exists - this might be OK
    echo "Table already exists, continuing...\n";
    
} catch (UnauthorizedException $e) {
    // Permission denied
    throw new \RuntimeException("Insufficient permissions: " . $e->getMessage());
    
} catch (SyntaxErrorException $e) {
    // CQL syntax error
    throw new \RuntimeException("Invalid CQL syntax: " . $e->getMessage());
    
} catch (InvalidException $e) {
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
            
        // RequestTimeoutException belongs here too when the operation is safe to
        // run twice: the connection stays open and keeps its prepared
        // statements, only the request that ran out is finished, and the node
        // was not blamed for the timeout — so the retry costs no reconnect.
        } catch (UnavailableException | ReadTimeoutException | WriteTimeoutException | OverloadedException $e) {
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
use Cassandra\Exception\ServerException\{ReadTimeoutException, WriteTimeoutException};

try {
    $result = $conn->query(
        'SELECT * FROM large_table WHERE complex_condition = ?',
        [$condition],
        Consistency::QUORUM
    )->asRowsResult();
    
} catch (ReadTimeoutException $e) {
    $ctx = $e->getErrorContext();
    $consistency = $ctx->consistency;
    $received = $ctx->nodesAnswered;
    $required = $ctx->nodesRequired;
    $dataPresent = $ctx->dataPresent;
    
    error_log("Read timeout: got {$received}/{$required} responses at {$consistency->name}, data_present: " . 
              ($dataPresent ? 'yes' : 'no'));
    
    return $conn->query(
        'SELECT * FROM large_table WHERE complex_condition = ?',
        [$condition],
        Consistency::ONE
    )->asRowsResult();
    
} catch (WriteTimeoutException $e) {
    $ctx = $e->getErrorContext();
    $consistency = $ctx->consistency;
    $received = $ctx->nodesAcknowledged;
    $required = $ctx->nodesRequired;
    $writeType = $ctx->writeType->value;
    
    error_log("Write timeout: got {$received}/{$required} responses at {$consistency->name}, write_type: {$writeType}");
    
    if ($writeType === 'BATCH_LOG') {
        error_log("Batch log write timeout - operation may have succeeded");
    }
}
```

### Error Information Access

Most server exceptions provide additional context:

```php
use Cassandra\Exception\ServerException\{UnavailableException, ReadTimeoutException, WriteTimeoutException};

try {
    $conn->query('SELECT * FROM users');
    
} catch (UnavailableException $e) {
    $ctx = $e->getErrorContext();
    echo "Consistency: " . $ctx->consistency->name . "\n";
    echo "Required: " . $ctx->nodesRequired . "\n";
    echo "Alive: " . $ctx->nodesAlive . "\n";
    
} catch (ReadTimeoutException $e) {
    $ctx = $e->getErrorContext();
    echo "Consistency: " . $ctx->consistency->name . "\n";
    echo "Received: " . $ctx->nodesAnswered . "\n";
    echo "Required: " . $ctx->nodesRequired . "\n";
    echo "Data present: " . ($ctx->dataPresent ? 'yes' : 'no') . "\n";
    
} catch (WriteTimeoutException $e) {
    $ctx = $e->getErrorContext();
    echo "Write type: " . $ctx->writeType->value . "\n";
    echo "Consistency: " . $ctx->consistency->name . "\n";
    echo "Received: " . $ctx->nodesAcknowledged . "\n";
    echo "Required: " . $ctx->nodesRequired . "\n";
}
```

Configuration Reference
-----------------------

### Connection Configuration

#### Node Configuration

**StreamNodeConfig** (supports SSL/TLS, persistent connections)
```php
use Cassandra\Connection\StreamNodeConfig;

$node = new StreamNodeConfig(
    host: 'cassandra.example.com',        // Can include protocol (tls://)
    port: 9042,                           // Port number
    username: 'user',                     // Username (optional)
    password: 'secret',                   // Password (optional)
    connectTimeoutInSeconds: 10,          // Connection timeout (default: 5)
    timeoutInSeconds: 15,                 // I/O timeout (default: 15); fractional values allowed, 0 disables it
    persistent: true,                     // Use persistent connections
    sslOptions: [                         // SSL/TLS options (see PHP SSL context); a non-empty array enables TLS
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

**SocketNodeConfig** (requires `ext-sockets`)
```php
use Cassandra\Connection\SocketNodeConfig;

$node = new SocketNodeConfig(
    host: '127.0.0.1',                    // Cassandra host
    port: 9042,                           // Cassandra port (default: 9042)
    username: 'cassandra',                // Username (optional)
    password: 'cassandra',                // Password (optional)
    // See [PHP socket_get_option documentation](https://www.php.net/manual/en/function.socket-get-option.php)
    socketOptions: [                      // Socket-specific options
        SO_RCVTIMEO => ['sec' => 15, 'usec' => 0],  // Receive timeout (default: 15)
        SO_SNDTIMEO => ['sec' => 10, 'usec' => 0],  // Send timeout (default: 10)
        SO_KEEPALIVE => 1,                // Keep-alive
    ],
    connectTimeoutInSeconds: 5            // Connection timeout (default: 5)
);
```

Both timeout components are honoured, so sub-second timeouts work (`['sec' => 0, 'usec' => 500000]` is half a second). Setting both to `0` disables the timeout, matching the meaning of the socket option itself — but see [Choosing timeout values](#choosing-timeout-values) before disabling the *receive* timeout, which is also what lets request timeouts and heartbeats be checked. `connectTimeoutInSeconds` is separate and must stay positive, so an unreachable host can never wedge the client indefinitely.

### Choosing timeout values

There are two independent layers, and they answer different questions.

**Transport timeouts** (`SO_RCVTIMEO`/`SO_SNDTIMEO`, `timeoutInSeconds`) are **stall** timeouts: they bound how long the connection makes *no progress at all*, not how long a request takes end to end. Sending a large batch or reading a large result set over a slow link therefore does not trip them, and there is no implicit ceiling on payload size.

**The request timeout** (`ConnectionOptions::$requestTimeoutInSeconds`, default 30s) is what governs a slow query. A server that is simply thinking sends nothing, which looks exactly like a stalled connection, so the transport timeout alone cannot tell the two apart — it keeps waiting, and only the request timeout decides when to give up, with a `RequestTimeoutException`. Adjust it per situation:

```php
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Request\Options\BatchOptions;

// TRUNCATE: Cassandra allows itself 60s for it (truncate_request_timeout),
// so the client has to allow more than that or it gives up on a server that
// was still working and would have answered.
$conn->query('TRUNCATE big_table', options: new QueryOptions(requestTimeoutInSeconds: 90.0));

// A full-table scan or an aggregate has no coordinator timeout to lean on —
// it is bounded by how much data it walks, so pick a value from the query,
// not from the cluster config.
$conn->query('SELECT count(*) FROM events', options: new QueryOptions(requestTimeoutInSeconds: 300.0));

// Schema changes wait for every node to agree on the new schema, which takes
// as long as the slowest node needs.
$conn->query('CREATE INDEX ON events (user_id)', options: new QueryOptions(requestTimeoutInSeconds: 120.0));

// A large batch is one request that the coordinator fans out and waits for,
// so give it more room than a single write.
$batch = $conn->createBatchRequest(options: new BatchOptions(requestTimeoutInSeconds: 60.0));
$batch->appendQuery('INSERT INTO events (id, v) VALUES (?, ?)', [$id, $v]);
$conn->batch($batch);

// Or change it for everything that follows, e.g. for a maintenance script:
$conn->setRequestTimeout(120.0);
```

`requestTimeoutInSeconds` is available on `QueryOptions`, `ExecuteOptions`, `BatchOptions` and `PrepareOptions`. Precedence runs from the most specific to the least: an explicit argument to `syncRequest()` or `asyncRequest()`, then the request's own options, then `setRequestTimeout()`, then `ConnectionOptions`.

The explicit argument bounds each request the call sends, not the call as a whole — when the driver has to prepare or reprepare the statement first, the `PREPARE` and the request it precedes each get the full budget:

```php
// The server may take up to 90s to answer, per request sent
$conn->syncRequest(new Query('TRUNCATE big_table'), requestTimeoutInSeconds: 90.0);

// The same override for an async statement, whose budget starts now
$statement = $conn->asyncRequest(new Query('SELECT * FROM huge'), requestTimeoutInSeconds: 300.0);
```

The default of 30s is chosen to sit above Cassandra's own coordinator timeouts, so that the server answers with a proper error before the client gives up. Raise it for anything the server itself allows more time for, or that is bounded by data volume rather than by a server-side limit:

| Operation | Suggested | Why |
|---|---|---|
| ordinary reads and writes | 30s (default) | above `read_request_timeout` (5s), `write_request_timeout` (2s) and `range_request_timeout` (10s) |
| `TRUNCATE` | 90s | `truncate_request_timeout` is 60s server-side |
| DDL / schema changes | 120s | waits for schema agreement across all nodes |
| full scans, aggregates, large batches | 300s or more | bounded by how much data is walked, not by a server-side timeout |

Raising the request timeout is safe: a connection that has actually died is still caught within about 35s by the heartbeat, rather than being left to the request timeout to notice.

#### Request budgets vs. wait bounds

Two different things are being bounded, and the API keeps them apart by name:

| | Set by | Means | On expiry |
|---|---|---|---|
| **Request budget** — `requestTimeoutInSeconds` | `ConnectionOptions`, `setRequestTimeout()`, request options, `syncRequest()` | how long the *server* may take to answer a request | the request is given up on: `RequestTimeoutException` |
| **Wait bound** — `timeoutInSeconds` | the `waitFor…()` methods | how long *this call* may block | the call returns empty-handed; nothing is given up on |

Every `waitFor…()` method takes the wait bound the same way:

| Value | Meaning |
|---|---|
| `null` | that method's default, see below |
| `0` | do not wait: return as soon as there is nothing more to read |
| `n` | wait at most `n` seconds |
| `INF` | wait for as long as it takes |

The default (`null`) is whatever makes sense for that wait:

| Method | `null` means |
|---|---|
| `waitForNextEvent()` | wait for as long as it takes — an event can arrive at any time |
| `waitForNextResponse()` | the connection's request timeout. *Not* "as long as it takes": with nothing in flight no response can ever arrive, so such a wait would never end |
| `waitForStatements()`, `waitForAnyStatement()`, `waitForAllPendingStatements()` | let the statements' own budgets bound the wait |

Note that `0` is the shortest wait, not an unlimited one — the opposite of what it means for the *transport* timeouts, where `SO_RCVTIMEO => ['sec' => 0, 'usec' => 0]` disables the timeout because that is what the socket option itself means. Even `0` still costs one read — a non-blocking one, so it does not wait on the transport either; for a look that never bounds itself by a deadline, use `tryReadNextEvent()` / `tryReadNextResponse()`, or `tryResolveStatement()` / `tryResolveStatements()` when waiting on statements.

Every wait is bounded by whichever comes first, and a request that runs out of budget is given up on from *any* wait — so it cannot quietly outlive its budget while you are, say, listening for events. Which wait *reports* it is a separate matter: only a call that was asked about that request raises it. `waitForNextEvent()` and `waitForNextResponse()` were asked for the next event or response, not about any request in particular, so they run their course; the caller finds out when they next touch the statement, which then throws `RequestTimeoutException`.

When several requests run out at the same moment they are all finished in one pass and reported as a single failure, which carries the statements themselves:

```php
try {
    $conn->waitForStatements($statements);
} catch (RequestTimeoutException $e) {
    foreach ($e->getTimedOutStatements() as $statement) {
        // exactly the requests that ran out, ready to be sent again
    }
}
```

A parked stream id is only released when its late answer arrives, so a node that keeps leaving requests unanswered would tie up more and more of them. Past `maxOrphanedStreams` the connection is closed and started over, and that *is* raised — as a `ConnectionException` — whatever the caller was waiting for: their connection is gone and the requests still in flight on it went with it.

Cassandra's own coordinator defaults are 5s for reads, 2s for writes, 10s for range requests and the generic request timeout, and **60s for `TRUNCATE`**. Keep the request timeout above whichever of those apply, so the server gets to answer with a proper error — which this driver surfaces as `ReadTimeoutException` / `WriteTimeoutException` — instead of the client giving up first.

A `RequestTimeoutException` leaves the connection open and does **not** count the node as failed, so one expensive query neither drops your prepared statements nor pushes a healthy node out of rotation. Only the request that ran out of time is finished.

Deadlines are handed to the read itself rather than merely consulted before one starts, so the transport timeout does not bound when a deadline is noticed: a request timeout of half a second fires after half a second even against a completely silent server whose receive timeout is 15s. A deadline that has already passed never buys a blocking read at all, so a request that ran out while you were busy elsewhere is reported at once, and a connection busy answering other requests cannot defer it either.

That makes the two settings genuinely independent, and disabling the receive timeout safe: `SO_RCVTIMEO => ['sec' => 0, 'usec' => 0]` and `timeoutInSeconds: 0` mean "no stall window", not "no deadlines" — request timeouts, wait bounds and the heartbeat all still fire on schedule. What you give up is the transport's own judgement that a connection making no progress at all has failed, which is the only thing that would end a wait carrying no deadline of its own on a connection with the heartbeat turned off.

**The heartbeat** (`heartbeatIntervalInSeconds`, default 30s) is what distinguishes a dead connection from a quiet one — at the socket, a coordinator that is still thinking and a node that vanished look identical. Whenever the connection has been silent for the interval, an `OPTIONS` frame is sent; because stream ids are multiplexed, it is answered on its own stream while a slow request is still being computed. A broken connection therefore surfaces after `heartbeatInterval + heartbeatTimeout` (~35s by default) as a `ConnectionException`, no matter how high the request timeout is — which is what makes generous request timeouts safe to set. Reads are bounded by when the probe is next due, so that holds during a wait with no deadline of its own and whatever the transport timeouts are set to.

Note that these three values are independent: the request timeout follows your cluster's coordinator timeouts, while the heartbeat interval should stay below the idle timeout of any NAT, firewall or load balancer between you and the node. They both default to 30s by coincidence, not because they are related.

#### Timeouts and async statements

The budget of an async statement runs from the moment its request was written to the node, not from the moment you get around to waiting for it — so a statement gets the same total allowance whether you wait immediately or after other work:

```php
$statement = $conn->queryAsync('SELECT * FROM big_table');
doSomethingElseFor(10);          // eats into the same 30s budget
$conn->waitForStatements([$statement]);   // ~20s left, not a fresh 30s
```

Each statement carries the timeout its own request asked for, so a mixed batch is not forced onto one number:

```php
$fast = $conn->queryAsync('SELECT * FROM users WHERE id = ?', [$id]);
$slow = $conn->queryAsync('SELECT * FROM huge', options: new QueryOptions(requestTimeoutInSeconds: 120));

$conn->waitForStatements([$fast, $slow]);   // each held to its own budget
```

When several statements are waited on together, the wait ends as soon as the *first* of them exhausts its own budget.

The polling methods (`tryResolveStatement()`, `tryReadNextResponse()`, …) never wait, so how long *your* loop runs is yours to bound — but they do keep the same books a wait does. Request budgets are still enforced, so a statement you only ever poll runs out of time and releases its stream id rather than staying pending for good; and the heartbeat is still sent, so a connection that died quietly is noticed by an application that never blocks. `Statement::tryGetResult()` and its siblings therefore raise `RequestTimeoutException` once the budget is gone, exactly as the blocking calls do. To look at a statement without any of that, use `Statement::peekResponse()`, which only reads what has already arrived.

When an async statement times out, only that statement is finished — the connection stays open and every other statement in flight on it keeps waiting:

```php
$slow = $conn->queryAsync('SELECT * FROM huge');
$fast = $conn->queryAsync('SELECT * FROM users WHERE id = ?', [$id]);

try {
    $conn->waitForStatements([$slow]);
} catch (RequestTimeoutException $e) {
    // $slow is finished ($slow->isTimedOut() === true), but the connection and
    // $fast are untouched:
    $fast->getResult();
}
```

What makes that safe is that the timed-out statement's stream id is *not* returned to the pool — the server may still answer on it, and reusing it would let that late answer resolve a different request. The id is parked until the late answer arrives, then released. A connection whose requests keep timing out would tie up more and more ids, so past `maxOrphanedStreams` (default 24) the connection is closed instead.

Synchronous requests take their stream ids from the same pool, so a sync timeout is handled identically and also leaves the connection open — which matters because closing it would clear the prepared statement cache and force every prepared statement to be prepared again.

Closing a connection invalidates every statement still in flight on it — their stream ids meant something only on that connection. Those are marked abandoned, and touching one fails at once with a `StatementException` rather than waiting out another request timeout:

```php
if ($statement->isAbandoned()) {
    // The connection went away mid-flight — send the request again.
}
```

The `host` may be a hostname, an IPv4 literal, or an IPv6 literal in either bare (`::1`) or bracketed (`[::1]`) form. It is resolved with `getaddrinfo()`, so IPv4-only, IPv6-only and dual-stack hosts all work; when a name resolves to several addresses, each is tried in turn until one connects. URL schemes (`tcp://`, `tls://`) are not accepted here — use `StreamNodeConfig` for TLS.

#### Connection Options

```php
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Connection\NodeSelectionStrategy;

$options = new ConnectionOptions(
    enableCompression: true,              // Enable LZ4 compression (default: false)
    throwOnOverload: true,                // Throw on server overload (v4+, default: false)
    nodeSelectionStrategy: NodeSelectionStrategy::RoundRobin, // Node selection (default: Random)
    preparedResultCacheSize: 200,         // Prepared statement cache size (default: 100)
    requestTimeoutInSeconds: 30,          // How long to wait for a server answer (default: 30, null = forever)
    maxOrphanedStreams: 24,               // Timed-out async statements a connection may accumulate (default: 24)
    heartbeatIntervalInSeconds: 30,       // OPTIONS heartbeat while idly waiting for events (default: 30, null = off)
    heartbeatTimeoutInSeconds: 5,         // How long a heartbeat may go unanswered (default: 5)
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
    defaultTimestamp: 1640995200000000,   // Default timestamp (microseconds, default: null)
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
    defaultTimestamp: 1640995200000000,   // Microseconds since epoch
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
use Cassandra\Value\EncodeOption\UuidEncodeOption;
use Cassandra\Value\EncodeOption\VarintEncodeOption;

$conn->configureValueEncoding(new ValueEncodeConfig(
    dateEncodeOption: DateEncodeOption::AS_DATETIME_IMMUTABLE,
    durationEncodeOption: DurationEncodeOption::AS_DATEINTERVAL,
    timeEncodeOption: TimeEncodeOption::AS_DATETIME_IMMUTABLE,
    timestampEncodeOption: TimestampEncodeOption::AS_DATETIME_IMMUTABLE,
    // uuid / timeuuid: AS_STRING (default) decodes to the canonical
    // 36-character string; AS_BINARY decodes to the raw 16-byte form, which
    // skips hex formatting and is worth it for large UUID-keyed result sets.
    uuidEncodeOption: UuidEncodeOption::AS_STRING,
    varintEncodeOption: VarintEncodeOption::AS_STRING,
));
```

#### Event Listeners
```php
use Cassandra\EventListener;
use Cassandra\WarningsListener;

// Event listener
$conn->registerEventListener(new class implements EventListener {
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

A: No. The library requires no PHP extension and works on a minimal PHP build, but some extensions enhance functionality:
- `ext-sockets`: Required for `SocketNodeConfig` (alternative: `StreamNodeConfig`, which needs no extension)
- `ext-openssl`: Required for `tls://` connections configured with `StreamNodeConfig`
- `ext-lz4`: Much faster native LZ4 (de)compression; a pure-PHP implementation is used otherwise
- `ext-gmp` or `ext-bcmath`: Faster large integer math for the `Varint` and `Decimal` types; a pure-PHP calculator is used otherwise

**Q: Can I run this on 32-bit PHP?**

A: Yes, with limited support. The following features are unsupported on 32-bit PHP: value types `Bigint`, `Counter`, `Date`, `Duration`, `Time`, `Timestamp`, and the `defaultTimestamp` request option. Use 64-bit PHP for full compatibility.

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

**Q: How do I add or remove items from a set, list, or map?**

A: Use CQL `UPDATE` with `+` / `-` and bind the delta as `?` (not `{ ? }`). See [Collection updates](#collection-updates) for set/map/list examples and a syntax table.

**Q: How do I work with timestamps?**

A: Use the Timestamp value class:
```php
use Cassandra\Value\Timestamp;

// Current time
$now = Timestamp::now();

// From string
$timestamp = Timestamp::fromValue('2024-01-15T10:30:00Z');

// From Unix timestamp
$timestamp = Timestamp::fromValue(1705312200000); // milliseconds
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
use Cassandra\Connection\StreamNodeConfig;

$conn = new Connection([
    new StreamNodeConfig('127.0.0.1', 9042, 'username', 'password')
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

Connection tuning examples
--------------------------

```php
use Cassandra\Connection;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\Connection\ConnectionOptions;

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

// Socket with custom timeouts
$socket = new SocketNodeConfig(
    host: '127.0.0.1',
    port: 9042,
    username: 'user',
    password: 'secret',
    // See [PHP socket_get_option documentation](https://www.php.net/manual/en/function.socket-get-option.php)
    socketOptions: [
        SO_RCVTIMEO => ['sec' => 15, 'usec' => 0],
        SO_SNDTIMEO => ['sec' => 10, 'usec' => 0],
    ],
    connectTimeoutInSeconds: 5,
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

$conn->registerEventListener(new class () implements EventListener {
    public function onEvent(Event $event): void {
        // enqueue to worker, react to topology/status/schema changes
    }
});

// Blocks until an event arrives, so no polling or backoff is needed
while (true) {
    $event = $conn->waitForNextEvent();
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

Benchmarks
----------

The following results were produced by the benchmarking suite in `benchmarks/` (Dockerized), comparing this library against the legacy DataStax PHP driver and the ScyllaDB PHP driver. Full setup and reproduction steps are documented in `benchmarks/README.md`; raw outputs are stored under `benchmarks/results/`.

```
================================================
Detailed Comparison
================================================

=== Benchmark Descriptions ===

benchInsertAndSelectWithoutTypeInfo
  100 inserts + 100 selects per round (without type hints)
  -> 1 iteration = 100 rounds, testing with 30 iterations

benchInsertAndSelectWithTypeInfo
  100 inserts + 100 selects per round (with type hints)
  -> 1 iteration = 100 rounds, testing with 30 iterations

benchPagedQuery
  1 paged query per round (500 rows, page size 50)
  -> 1 iteration = 100 rounds, testing with 30 iterations

benchPreparedInsert
  100 inserts per round (prepared statement)
  -> 1 iteration = 100 rounds, testing with 30 iterations

benchSimpleSelect
  1 simple select per round
  -> 1 iteration = 700 rounds, testing with 30 iterations


=== Performance Comparison (avg time per iteration, lower is better) ===
==================================================================================================================================
Benchmark                                     php-cassandra   DataStax        ScyllaDB        vs DataStax          vs ScyllaDB    
----------------------------------------------------------------------------------------------------------------------------------
benchInsertAndSelectWithoutTypeInfo           3.9017s         6.8185s         7.4834s         1.75x faster         1.92x faster   
benchInsertAndSelectWithTypeInfo              3.8260s         6.6074s         6.8760s         1.73x faster         1.80x faster   
benchPagedQuery                               251.83ms        561.52ms        554.07ms        2.23x faster         2.20x faster   
benchPreparedInsert                           1.8196s         3.0907s         3.0200s         1.70x faster         1.66x faster   
benchSimpleSelect                             152.25ms        329.02ms        359.00ms        2.16x faster         2.36x faster   
==================================================================================================================================

Notes:
  Times are average per iteration. Each iteration runs multiple rounds of operations.
  'Xx faster/slower' compares php-cassandra to the other driver (lower time is better)
  php-cassandra: PHP 8.5 | DataStax: PHP 7.1 | ScyllaDB: PHP 8.5
```

Notes:
- The DataStax driver runs on PHP 7.1; ScyllaDB and php-cassandra ran on PHP 8.5.
- Environment details and exact commands are in `benchmarks/README.md`.

Version support
---------------

- Protocol versions **v3**, **v4**, and **v5** are supported. Features like per-request `keyspace` / `now_in_seconds` require protocol **v5**.
- Cassandra **3.0**, **3.11**, **4.x**, **5.x** and ScyllaDB **6.2, 2025.1, 2025.2, 2025.3** are part of the regular test matrix.

### Server compatibility and required settings

| Server version | Supported? | Protocol(s) | Notes / required settings |
|----------------|------------|-------------|---------------------------|
| **Apache Cassandra 2.1.x** | ✅ With manual configuration | v3 | Cassandra 2.1 only supports protocol v3 and does **not** support protocol negotiation. You must set the initial protocol explicitly: `new ConnectionOptions(initialProtocolVersion: ProtocolVersion::V3)`. Optionally, also restrict `allowedProtocolVersions` to `[ProtocolVersion::V3]`. |
| **Apache Cassandra 2.2.x / 3.x** | ✅ (3.0 / 3.11 tested) | v4 | These versions speak protocol v4 but do not support protocol negotiation. The default `initialProtocolVersion` is `ProtocolVersion::V4`, so no special configuration is required. |
| **Apache Cassandra 4.x** | ✅ (tested) | v4 / v5 | Protocol negotiation is supported from 4.x; the driver will automatically negotiate the highest mutually supported protocol (v5, then v4, then v3) using the default `ConnectionOptions`. |
| **Apache Cassandra 5.x** | ✅ (tested) | v5 | Fully supported with protocol negotiation. No special configuration is required; v5 will be negotiated when available. |
| **ScyllaDB 6.2, 2025.1, 2025.2, 2025.3** | ✅ (tested) | v4 | ScyllaDB currently supports protocol v4 and does **not** support protocol negotiation. The default `initialProtocolVersion` is `ProtocolVersion::V4`, so no special configuration is required. |

For example, to connect to a Cassandra **2.1** cluster:

```php
use Cassandra\Connection;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Protocol\ProtocolVersion;

$nodes = [
    new StreamNodeConfig(
        host: '127.0.0.1',
        port: 9042,
        username: 'cassandra',
        password: 'cassandra',
    ),
];

$options = new ConnectionOptions(
    initialProtocolVersion: ProtocolVersion::V3,
    // Optional but recommended for Cassandra 2.1:
    // allowedProtocolVersions: [ProtocolVersion::V3],
);

$conn = new Connection($nodes, keyspace: 'my_keyspace', options: $options);
$conn->connect();
```

#### Server-side features that are off by default

A few CQL features need to be enabled on the server before any driver can use them. They are pure server configuration — nothing changes on the client side.

| Feature | Apache Cassandra | ScyllaDB |
|---|---|---|
| User-defined functions / aggregates | `user_defined_functions_enabled: true` in `cassandra.yaml` (called `enable_user_defined_functions` in 4.0) | `--experimental-features=udf` **and** `--enable-user-defined-functions=true`; bodies are written in Lua, not Java |
| Materialized views | `materialized_views_enabled: true` in `cassandra.yaml` (called `enable_materialized_views` in 4.0); every `CREATE MATERIALIZED VIEW` returns a warning that the feature is experimental | Enabled by default |
| SASI indexes (`CREATE CUSTOM INDEX … USING 'org.apache.cassandra.index.sasi.SASIIndex'`, `LIKE`) | `sasi_indexes_enabled: true` in `cassandra.yaml` (called `enable_sasi_indexes` in 4.0) | Not supported |


API reference (essentials)
--------------------------

- `Cassandra\Connection`
  - `connect()`, `disconnect()`, `isConnected()`, `getProtocolVersion()`
  - `setConsistency(Consistency)`, `withConsistency(Consistency)`
  - `setKeyspace(string)`, `withKeyspace(string)`, `supportsKeyspaceRequestOption()`, `supportsNowInSecondsRequestOption()`
  - `query(string, array = [], ?Consistency, QueryOptions)` / `queryAsync(...)` / `queryAll(...)`
  - `prepare(string, PrepareOptions)` / `prepareAsync(...)`
  - `execute(Result $previous, array = [], ?Consistency, ExecuteOptions)` / `executeAsync(...)` / `executeAll(...)`
  - `batch(Batch)` / `batchAsync(Batch)`
  - `syncRequest(Request, ?float $requestTimeoutInSeconds)` / `asyncRequest(Request, ?float $requestTimeoutInSeconds)`
  - `waitForStatements(array $statements, ?float $timeoutInSeconds)` / `waitForAllPendingStatements(?float $timeoutInSeconds)` / `waitForAnyStatement(array $statements, ?float $timeoutInSeconds): ?Statement`
  - `registerEventListener(EventListener)` / `unregisterEventListener(EventListener)` / `waitForNextEvent(?float $timeoutInSeconds): ?Event`
  - `registerWarningsListener(WarningsListener)` / `unregisterWarningsListener(WarningsListener)`
  - `waitForNextResponse(?float $timeoutInSeconds): ?Response` — both waits return null when the timeout elapses with nothing to report
  - `setRequestTimeout(?float)`
  - Non-blocking helpers: `drainAvailableResponses()`, `tryResolveStatement()`, `tryResolveStatements()`, `tryReadNextResponse()`, `tryReadNextEvent()`

- Results
  - `RowsResult` (iterable): `fetch()`, `fetchAll()`, `fetchColumn()`, `fetchAllColumns()`, `fetchKeyPair()`, `fetchAllKeyPairs()`, `configureFetchObject()`, `fetchObject()`, `fetchAllObjects()`, `getRowsMetadata()`, `hasMorePages()`
  - `PreparedResult` (for execute)
  - `SchemaChangeResult`, `SetKeyspaceResult`, `VoidResult`

- Types
  - `Cassandra\Consistency` (enum)
  - `Cassandra\SerialConsistency` (enum)
  - `Cassandra\Type` (enum) and `Cassandra\Value\*` classes (Ascii, Bigint, Blob, Boolean, Counter, Date, Decimal, Double, Duration, Float32, Inet, Int32, ListCollection, MapCollection, NotSet, SetCollection, Smallint, Time, Timestamp, Timeuuid, Tinyint, Tuple, UDT, Uuid, Varchar, Varint, Vector, ...)

Changelog
---------

See `CHANGELOG.md` for release notes and upgrade considerations.

License
-------

This library is released under the MIT License. See `LICENSE` for details.


Contributing
------------

Contributions are welcome! Here's how to get started:

### Development Setup

1. **Fork and Clone**
   ```bash
   git clone https://github.com/MichaelRoosz/php-cassandra.git
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

Docker quickstart for integration tests:
```bash
composer test:integration:up           # start Cassandra test container (ports 9142->9042)
composer test:integration:init         # wait until Cassandra is ready
composer test:integration:run          # run integration suite (socket + stream)
composer test:integration:down         # stop and clean up
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
