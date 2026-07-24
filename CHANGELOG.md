## Unreleased

### Added
* `Cassandra\Compression\Lz4Compressor`: a pure-PHP LZ4 block compressor. When compression is negotiated, outgoing request frames are now compressed as well as incoming responses — previously only responses were decompressed and requests were always sent uncompressed.
* Optional native LZ4 acceleration: when the `lz4` PHP extension is installed it is used automatically for block (de)compression (much faster), with the pure-PHP implementation as a transparent fallback.

### Fixed
* `Cassandra\Connection\FrameCodec`: a request larger than the maximum frame payload size (128 KiB) triggered an infinite loop, because the chunking offset and remaining-length counters were reset on every iteration. Large requests are now split into frames correctly.
* `Cassandra\Connection\Socket` / `Cassandra\Connection\Stream`: the partial-write loop used `while ($data)`, which treats a remaining single byte `"0"` (`0x30`) as falsy and stops early, silently dropping that byte and sending a truncated frame to the server. The loop now terminates on `$data !== ''`.
* `Cassandra\Connection\FrameCodec`: a request whose serialized size was exactly the maximum frame payload size (131071 bytes) was sent through the multi-frame path as a single non-self-contained frame instead of one self-contained frame. The boundary is now handled by the single-frame path.
* `Cassandra\Connection\FrameCodec`: the frame reader returned early on a zero-length payload without consuming the 4-byte payload CRC32 trailer, while `writeFrame()` always emits one. A zero-payload frame from the server would leave 4 stray bytes in the stream and fail the next header CRC24. A zero-length payload is no longer treated as a special case; its CRC32 trailer is read and verified like any other frame's.
* `Cassandra\Request\Query` / `Cassandra\Request\Execute`: the `serialConsistency` request option was encoded as the enum instance instead of its protocol code, so any query using it was rejected by the server with "Invalid consistency for conditional update". Lightweight transactions with an explicit `SerialConsistency` now work.
* `Cassandra\Connection`: cached prepared statements could not be reprepared after the server invalidated them (for example following a schema change). A cache hit returned a result with no associated request, and the repreparation then consulted the same stale cache entry and looped forever. Prepared statements now reprepare transparently after invalidation.
* `Cassandra\Connection::queryAsync()`: an asynchronous query that relied on auto-preparation (any native, untyped bind value) failed on protocol v5 with `Server protocol version does not support request option "keyspace"`, because the internally built prepare request was left at the default protocol version and stream id.
* `Cassandra\Connection::waitForAnyStatement()`: hung when every supplied statement had already been resolved, because it blocked on a response before checking readiness.
* `Cassandra\Connection::disconnect()`: unresolved asynchronous statements from the previous connection were left registered, so `waitForAllPendingStatements()` blocked forever after a reconnect. The per-connection stream state is now reset on disconnect.
* `Cassandra\Response\StreamReader::readValue()`: zero-length ("empty") values for fixed-length types desynced the row decoder. Cassandra distinguishes null (`[bytes]` length `-1`) from empty (length `0`), and an empty value is legal for every fixed-length type. Both the fast path (`INT`, `DOUBLE`, `BOOLEAN`, `UUID`, `BIGINT`, …) and the `Cassandra\Value\ValueWithFixedLength` path (`TIMESTAMP`, `DATE`, `TIME`, `SMALLINT`, `TINYINT`, `FLOAT`, …) ignored the declared length and always read a fixed number of bytes, consuming bytes belonging to the next column and misaligning the rest of the row and every remaining row in the page. `readValue()` is now authoritative about the cell frame: it maps a zero length to `null` (consistent with `-1`/`-2`) and forces the stream offset to the end of the declared value after decoding, and `Cassandra\Value\ValueWithFixedLength::fromStream()` honours the passed length.
* Fixed several misspelled exception context keys (`proocol_versions_*` → `protocol_versions_*`, `required_protocol_verison` → `required_protocol_version`).
* `Cassandra\Connection\ResponseReader`: the frame header's stream id was decoded as an unsigned `[short]`, but the protocol defines it as signed and server-initiated responses (events) use `-1`. As a result `Cassandra\Response\Response::getStream()` reported `65535` for every event, giving misleading values in error contexts and exception payloads. The stream id is now sign-extended to its correct negative value.
* `Cassandra\VIntCodec::decodeUnsignedVint64()`: the decoder read the continuation bytes indicated by the leading byte without checking that the input actually contained them. On truncated input PHP emitted an "undefined array key" warning and treated the missing byte as `0`, silently returning a wrong integer. The decoder now validates the length up front and throws a `Cassandra\Exception\VIntCodecException`, mirroring the stream-based `readUnsignedVint64()`.
* `Cassandra\Connection\Socket::write()`: after a partial write the inner loop re-invoked `socket_write()` without re-entering `selectSocketForWrite()`, so against a slow peer it busy-spun on `EAGAIN`/zero-byte writes (burning CPU) until `sendTimeout` elapsed. The loop now returns to the outer loop and waits for the socket to become writable again before retrying.
* `Cassandra\Connection\Socket::connect()`: the socket was always created with `AF_INET`, so connecting to an IPv6 host failed. The address family is now chosen based on the configured host, using `AF_INET6` for IPv6 literals.
* `Cassandra\Value\UDT` / `Cassandra\Value\Tuple`: a serialized value carrying fewer fields than its type declares (for example UDT rows written before an `ALTER TYPE ... ADD`) was decoded by reading past the value's declared length into whatever bytes followed, desyncing the row decoder. Decoding now stops at the declared length and null-fills the remaining trailing fields.
* `Cassandra\Value\Vector`: fixed two element-framing bugs. `vector<smallint>`/`vector<tinyint>` are variable-length inside vectors ([CASSANDRA-14476](https://issues.apache.org/jira/browse/CASSANDRA-14476)), but `fromStream()` expected no length prefix and misread them; and `getBinary()` wrote the variable-length prefix as a two's-complement varint rather than an unsigned VInt, corrupting elements of 128 bytes or more (e.g. a long `vector<text>`). Both paths now use unsigned-VInt prefixes and agree.

### Performance
* Response and value dispatch maps (`Cassandra\Response\Response`, `Result`, `Error`, `Event`, `Cassandra\ValueFactory`, `Cassandra\TypeNameParser`) are now lazily cached instead of being rebuilt on every call.
* `Cassandra\Response\StreamReader::readValue()`: scalar, list and set cells are now decoded directly without allocating an intermediate `Value` object per cell.

### Tests
* Added integration tests for collection updates (incremental `+`/`-`, prepend, indexed assignment, single-element deletion, clearing, TTL, conditional updates, and frozen and nested collections), lightweight transactions, CQL JSON (`INSERT JSON`, `SELECT JSON`, `fromJson()`/`toJson()`, `DEFAULT UNSET`/`DEFAULT NULL`), user-defined functions and aggregates, materialized views, secondary indexes (including `KEYS()`, `ENTRIES()`, `FULL()` and SASI/`LIKE` index targets), authentication (roles, `GRANT`/`REVOKE`, permissions), and further CQL features (tracing, virtual tables, TTL/`WRITETIME`, schema evolution, aggregates, static columns and counter batches).
* Added regression tests for prepared-statement repreparation, asynchronous auto-preparation, `waitForAnyStatement()` and reconnect after `disconnect()`.
* Added integration tests for the ScyllaDB-only `BYPASS CACHE` and `USING TIMEOUT` CQL extensions, asserting both that they work on ScyllaDB and that Apache Cassandra rejects them.
* Added a unit test for the binary encoding of the `serialConsistency` request option.
* Added integration tests for empty (zero-length) values of fixed-length types, asserting that empty values are decoded as `null` and do not desync the row decoder.
* Added regression tests for `vector<smallint>`/`vector<tinyint>` framing and for variable-length vector elements of 128 bytes or more (`vector<text>`).
* Added compression benchmarks running against a real Cassandra/ScyllaDB node with several-megabyte payloads: `benchmarks/CompressionBench.php` (phpbench) measures a large-blob round-trip with compression on vs. off, and `benchmarks/compression-network-bench.php` compares the two across a range of simulated network bandwidths (via a bandwidth-throttling transport) to show where compression starts to pay off.
* The test containers now enable user-defined functions, materialized views and SASI indexes (disabled by default), and a dedicated `docker-compose.auth.yml` runs the suite against an authenticated cluster.
* Extended the CI test matrix to cover PHP 8.5 and ScyllaDB 2026.1 and 2026.2.

### Documentation
* README: added a "Collection updates" section covering incremental `+`/`-` updates, prepend, indexed assignment, single-element deletion, clearing, and frozen and nested collections.
* README: added "Lightweight transactions (LWT)" and "JSON support" sections.
* README: documented the server-side settings required for user-defined functions, materialized views and SASI indexes.

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
