## v1.2.1

A maintenance release with the security, data and hang fixes from 2.0 that apply to 1.x. It changes no API. 2.0 has many more fixes; see [Not backported](#not-backported).

### Security
* A key `passphrase` in `sslOptions` no longer reaches the context of a failed connect.
* `sslOptions` other than the defaults on a host without a scheme connected in plaintext; they now connect over `tls://`.
* A keyspace name is interpolated into `USE`, so one with a semicolon ran CQL of its own. Names must be an unquoted or a double-quoted identifier (`CONNECTION_INVALID_KEYSPACE_NAME`).
* Response frames larger than the protocol's 256 MiB, LZ4 blocks expanding past their declared length, decimal scales beyond ±100000 and varints longer than 1701 bytes are refused instead of exhausting memory or time.

### Native errors
* `SocketNodeConfig` without ext-sockets raised PHP's "Undefined constant" error; it raises `SOCKET_EXTENSION_NOT_LOADED`.
* `Duration::asDateInterval()` of a zero duration raised a native exception on PHP 8.3+.

### Wrong data
* Empty cells of fixed-length types (`int`, `bigint`, `uuid`, `timestamp`, ...) read into the next cell and misread the rest of the row. They are now `null`, `''` for text and blob, and `[]` for collections.
* A `date` past either end of its range wrapped around to the other end; it is refused. Dates no longer depend on PHP's default timezone.
* A `duration` given as an inverted `DateInterval` with negative fields became `0s`.
* `Decimal::fromValue()` dropped the fraction of a float (`0.1` became `0`), and a negative scale decoded to the wrong value.
* `vector<smallint>` and `vector<tinyint>` decoded wrong values.
* The serial consistency of a QUERY or EXECUTE was sent as `ONE`.
* UDTs and tuples written before an `ALTER TYPE ... ADD` failed to read; the missing fields are `null`.
* `ResultIterator::next()` without `current()`, `FetchType::BOTH` with numeric column names, keyspace schema change events, empty `boolean` values, `ServerError` mapping and write timeout contentions were read wrongly.
* `Timestamp::getValue()` spells years beyond 9999 with a leading `+`, so the string parses back to the same value.
* A prepared statement could run as another one: the cache key joined the query and the keyspace without a separator.
* An untyped int bound to a query was truncated to 32 bits; one outside that range now raises `REQUEST_VALUES_INT_OUT_OF_INT32_RANGE`.
* Strings that are no duration, such as `1x2y` or `1h junk`, became `0s` or their prefix; they are refused.
* Impossible calendar dates such as `2020-02-30` rolled over into the next month; `Date` and `Timestamp` refuse them.
* A vector with more values than dimensions dropped the extra ones; a count that does not match raises `VALUE_VECTOR_DIMENSION_MISMATCH`.

### Hangs
* A v5 request of 128 KiB or more was written forever.
* A zero-length v5 segment desynchronized the stream.
* A write whose remaining data was the single byte `0` stopped short.
* `waitForAnyStatement()` hung on an empty list or an already answered statement; an empty list now raises `CONNECTION_WAIT_FOR_ANY_STATEMENT_EMPTY_LIST`.
* Events with stream id `-1` were read as stream `65535`.

### Not backported
These need changes too large for a patch release, mostly the reworked connection layer of 2.0; upgrade to fix them:
* Reads without any deadline: a peer that goes silent without closing the connection blocks a call indefinitely, and a signal during `stream_select()` drops a stream connection.
* Repreparation of cached statements, batches and paged results; heartbeats; stream id reuse after a connection is replaced.
* Frozen sets and maps in comparator order, maps with numeric-string or composite keys, case-sensitive quoted bind markers.
* IPv6 on the socket transport, and the remaining validation and LZ4 hardening.

## v1.2.0

This release improves compatibility with older Cassandra versions, fixes LZ4 compression for protocol v3/v4 (Cassandra 3.x and 2.x), and adds full support for ScyllaDB.

### Added
* Full support for ScyllaDB (6.2, 2025.1, 2025.2, 2025.3).

### Changed
* Initial protocol version while connecting is now V4 (was V3). To connect to Cassandra version 2.1, `initialProtocolVersion` in `ConnectionOptions` has to be set to `ProtocolVersion::V3`.

### Fixed
* Fixed lz4 decompression for protocol v3 and v4 (Cassandra 3 and older)

## v1.1.0

This release introduces the ability to override the default pool of allowed protocol versions (v5, v4, v3) via the new `allowedProtocolVersions` property in `ConnectionOptions`. This is a low-level feature intended for advanced use cases and should not be used by most users. The default behavior, which attempts to negotiate the highest supported version (v5 > v4 > v3), is recommended for the majority of situations.

All breaking changes affect low-level API only, which is unlikely to be used by most users.

### Breaking Changes
* Cassandra\Protocol\Header: type of `$version` changed from `int` to `Cassandra\Protocol\ProtocolVersion`
* These methods now expect for `$version` a value of type `Cassandra\Protocol\ProtocolVersion` instead of `int`:
    * `Cassandra\Connection\ResponseReader::readResponse()`
    * `Cassandra\Request\Request::__construct()`
    * `Cassandra\Request\Request::setVersion()`
* Exception context: type of `protocol_version` changed from `int` to `string`

### Changed
* Cassandra\Connection: method `getVersion()` is now deprecated, use `getProtocolVersion()` instead
* Cassandra\Protocol\Frame: method `getVersion()` is now deprecated, use `getProtocolVersion()` instead
* Cassandra\Request\Request: method `getVersion()` is now deprecated, use `getProtocolVersion()` instead
* Cassandra\Response\Response: method `getVersion()` is now deprecated, use `getProtocolVersion()` instead
* Changed visibility of some properties and methods from `protected` to `private` to clarify public api

### Added
* Added enum `Cassandra\Protocol\ProtocolVersion`
* Cassandra\Connection\ConnectionOptions: added property `allowedProtocolVersions` (array of enum `Cassandra\Protocol\ProtocolVersion` )
* Cassandra\Connection: added method `getProtocolVersion(): ProtocolVersion`
* Cassandra\Protocol\Frame: added method `getProtocolVersion(): ProtocolVersion`
* Cassandra\Request\Request: added method `getProtocolVersion(): ProtocolVersion`
* Cassandra\Response\Response: added method `getProtocolVersion(): ProtocolVersion`

### Removed
* Cassandra\Connection: removed method `onWarnings()`

## v1.0.1

This is a small bugfix, restoring compatibility with older PHP versions (8.1, 8.2, 8.3).

### Fixed 🔷
* Fixed compatibility with PHP 8.1, 8.2 and 8.3

## v1.0.0 "Prism" 🔷🌈

This release brings major improvements across the library.
In contrast to previous releases, it includes some breaking API changes. These were necessary to introduce more exciting features (including full support for the `Vector` data type 🤖) and to enhance and fix existing ones.

Upcoming 1.x releases will focus on stability and performance while keeping the API stable.

### Added 🔷
* Updated Asynchronous API, now providing additional helper methods (`waitForStatements`, `waitForAnyStatement`, `waitForAllPendingStatements`) and supporting non-blocking response processing (`drainAvailableResponses`, `tryResolveStatement`, `tryResolveStatements`, `tryReadNextResponse`)
* Events subsystem: blocking (`waitForNextEvent`) and non-blocking event polling (`tryReadNextEvent`)
* New `Value` capabilities including `Vector` data type support
* Support for configuring value encoding for data types with multiple encodings
* Warnings listener interface and registration
* Round-robin and random node selection strategies

### Changed 🔷
* Prepared statements: auto-prepare for typed execution, names-for-values auto-detection, re-prepare on UNPREPARED, metadata caching; helpers `executeAll`, pagination helpers
* Enforce a minimum `pageSize` of 100 in `QueryOptions` for efficient paging

### Fixed 🔷
* Numerous stability and correctness fixes across execute/query flows and pagination edge cases
* Richer server exception context (consistency levels, required/received counts, write types)

### Documentation 🔷
* Comprehensive README: quick start, configuration reference, async/event APIs, migration guide, error handling.

### Tests 🔷
* Added unit and integration tests for the critical parts of the library

## v0.8.1
* Allow types Date, Time, and Timestamp to be created from string values

## v0.8.0
* Improve API for prepared statements
* Fix a bug in `execute()` call
* Fix PHPDoc for `paging_state`
* Fix version calculation for server packets
* Replace `SplFixedArray` with `array`
