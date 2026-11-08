## v2.0.0

A major release: reworked timeouts, a heartbeat, pluggable authentication, request compression, lossless maps and several hundred correctness fixes. Most applications only need the first section of [UPGRADE.md](UPGRADE.md).

### Breaking changes

See [UPGRADE.md](UPGRADE.md) for what to change.

### Added

* **Request timeouts**: `requestTimeoutInSeconds` on `ConnectionOptions` and all request options, plus `setRequestTimeout()`. An expired request leaves the connection, its prepared statements and other requests untouched. `RequestTimeoutException::getTimedOutStatements()` names what to re-send.
* **Wait bounds**: every `waitFor…()` takes `$timeoutInSeconds`, with the constants `Connection::WAIT_DEFAULT`, `WAIT_NONE` and `WAIT_FOREVER`.
* **Heartbeat**: `heartbeatIntervalInSeconds` and `heartbeatTimeoutInSeconds` detect a dead connection after about 35s, however long a request may take.
* **Authentication extension point**: implement `Connection\Authenticator` for multi-round SASL mechanisms. Password authentication is `PasswordAuthenticator`.
* **Maps with any key**: `MapEncodeOption` and `MapCollection::fromEntries()` represent object and composite keys losslessly.
* **Request compression**: `Lz4Compressor` compresses outgoing frames as well. The `lz4` extension is used automatically when installed.
* **LZ4 frame dictionaries**: `Lz4Decompressor::setDictionary()` decodes v1 frames compressed with a dictionary, whether the frame names it by Dict-ID or not (as `lz4 -D` writes them).
* `UuidEncodeOption::AS_BINARY`, and raw 16-byte input for `Uuid`/`Timeuuid`.
* `maxOrphanedStreams`, and `SocketNodeConfig::$connectTimeoutInSeconds`.
* `Statement::isTimedOut()`, `isAbandoned()` and `peekResponse()`; `Connection::getKeyspace()` and `getRequestTimeout()`.
* Sub-second transport timeouts, where a timeout of zero means none.

### Changed

* `Cassandra\Connection` was split into a thin public API over `@internal` classes. No public method changed through this; only code using reflection on private state is affected.
* Deadlines bound the read itself, so they fire on time whatever the transport timeouts are, and use a monotonic clock, so a system clock change cannot fire or delay them (where `hrtime()` is disabled, the wall clock stands in). `Statement::getSentAt()` is on that clock too.
* Default transport timeouts: receive 15s (was 10s socket, 30s stream), socket send 10s (was 5s).
* An async statement's budget runs from when it was sent. Statements in flight when a connection closes are marked abandoned and fail at once.
* The non-blocking calls now enforce request budgets and send the heartbeat, so `Statement::tryGetResult()` can raise `RequestTimeoutException`.
* No PHP extension is required any more: `ext-ctype`, `ext-mbstring` and `ext-filter` are no longer used.
* Connecting tries the next node when the handshake fails, not only when TCP does.

### Fixed

**Security**

* A key `passphrase` in `sslOptions` no longer reaches the context of a failed connect, and passwords are hidden from `var_dump()` and, on PHP 8.2+, from stack traces.
* `setKeyspace()` closed a CQL injection through a keyspace name containing a semicolon.
* `sslOptions` on a host without a scheme silently connected in plaintext.
* Response frames larger than the protocol allows, deeply nested types and unbounded `Decimal` exponents are refused.

**Wrong data**

* A `date` past either end of its range, given as a string or a `DateTimeInterface`, wrapped around to the other end instead of being refused.
* A `duration` given as an inverted `DateInterval` with negative fields (`invert = 1` on `-5 days`) became `0s` instead of the `+5 days` PHP applies.
* Frozen sets and maps are serialized in comparator order, as Cassandra requires, and duplicates are refused.
* Empty values no longer desynchronize a row, and are decoded per type as Cassandra defines them.
* `Decimal` and `Varint` are canonical: floats keep their value, and leading zeros, signed zero and scientific notation round-trip correctly.
* `vector<smallint>`, `vector<tinyint>`, variable-length elements and truncated vectors are framed correctly.
* Maps with numeric-string, boolean, float or composite keys encode and decode.
* Short `UDT`/`Tuple` values read correctly; `Duration`, `Uuid` and `Decimal` strings are validated instead of silently accepted.
* Quoted bind marker case is preserved, and prepared values match their markers exactly, including after a repreparation.
* `FetchType::BOTH` with numeric column names, `ResultIterator` positions and `RowClass` `isset()` return the right values.
* `DateType`, `LegacyTimeUUIDType` and `LexicalUUIDType` columns are decoded by type instead of as raw bytes.
* `serialConsistency` was encoded incorrectly, and `executeAll()` now follows the metadata id of each page.

**Hangs and lost connections**

* Requests above the 128 KiB frame payload no longer loop forever on v5, and zero-length frames no longer desynchronize the stream.
* `waitForAnyStatement()` no longer hangs on an empty or already resolved list.
* A signal during `select` is retried rather than treated as a failure; a repeatedly failing `select` no longer retries forever.
* A partially read response no longer carries over to the next connection, and an answer is never matched to a wait on a replaced connection.
* A large response no longer trips the heartbeat, and exhausted stream ids no longer drop the connection.
* Repreparation works for cached statements, batches and async auto-prepare, with a bounded number of attempts.

**Transports**

* IPv6 and multi-address hosts work on the socket transport, and partial writes are retried correctly on both transports.
* Fractional and `usec` timeouts are honoured, and send timeouts bound stalls rather than the whole payload.
* The stream transport disables Nagle's algorithm, which could delay small requests by milliseconds.
* The pure-PHP LZ4 decoder validates frames, handles legacy multi-block frames and computes checksums correctly.

### Performance

* Rows decode without intermediate objects for scalars, lists, sets, `timestamp` and `date`. `timestamp` and `date` cells are about 2.5× faster.
* Primitive reads no longer copy their bytes, large responses no longer quadratically rebuild the read buffer, and waits no longer walk every statement in flight.
* The pure-PHP varint calculator is 7–17× faster.

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
