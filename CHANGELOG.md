## Unreleased

### Added
* `Cassandra\Value\EncodeOption\UuidEncodeOption`: `uuid`/`timeuuid` values can now be decoded either as the canonical 36-character string (`AS_STRING`, the default) or as the raw 16-byte binary form (`AS_BINARY`), selected via `ValueEncodeConfig(uuidEncodeOption: …)`. The binary form skips the hex-formatting step entirely, which is worth it when reading large result sets keyed by UUID; the string form remains the default for ergonomics.
* `Cassandra\Value\Uuid` / `Cassandra\Value\Timeuuid`: the constructor now also accepts the raw 16-byte binary form in addition to the canonical string, so a value read with `UuidEncodeOption::AS_BINARY` can be re-bound directly without a format→parse round-trip. The value is stored raw internally, making `getBinary()` a no-op and normalizing `getValue()` to the canonical lowercase string. The two forms are distinguished by length and cannot collide (a canonical string is always 36 characters).
* `Cassandra\Compression\Lz4Compressor`: a pure-PHP LZ4 block compressor. When compression is negotiated, outgoing request frames are now compressed as well as incoming responses — previously only responses were decompressed and requests were always sent uncompressed.
* Optional native LZ4 acceleration: when the `lz4` PHP extension is installed it is used automatically for block (de)compression (much faster), with the pure-PHP implementation as a transparent fallback.

### Changed
* The library now requires no PHP extension at all. `ext-ctype`, `ext-mbstring` and `ext-filter` were all previously used; all three uses have been removed rather than declared in `composer.json`, so the library keeps working on minimal PHP builds.
* `Cassandra\TypeNameParser` no longer uses `ext-mbstring`.
* `Cassandra\Connection\Socket` no longer uses `ext-filter`.
* `Cassandra\Value\Varint`, `Cassandra\TypeNameParser` and the decimal calculators no longer use `ext-ctype`.

### Fixed
* `Cassandra\Value\Date`: the number of days since the epoch was computed by diffing the supplied value against a `1970-01-01` base built in the ambient (default) timezone. For a `DateTimeInterface` carrying a time-of-day in a different timezone this could yield a date off by one day. Both ends are now anchored at UTC midnight and a `DateTimeInterface` is reduced to its own calendar date (`Y-m-d`), so the stored day count is exact and independent of `date_default_timezone_*`.
* `Cassandra\Value\ListCollection` / `Cassandra\Value\SetCollection`: `getBinary()` encoded every element through `ValueFactory::getBinaryByTypeInfo()`, which rejects a `Cassandra\Value\ValueBase` instance, so a list or set whose elements were already `Value` objects (for example a `list<duration>` or a `set<uuid>` built from `Value` objects, or a nested `list<tuple<…>>`) threw a `Cassandra\Exception\ValueException` during encoding — even though the same object works as a map value, UDT field or tuple element. List and set elements now honour a pre-built `ValueBase` via `getBinary()`, matching `Cassandra\Value\MapCollection`, `UDT` and `Tuple`.
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
* `Cassandra\Connection\Socket::connect()`: the socket was always created with `AF_INET`, so connecting to an IPv6 host failed. The host is now resolved up front with `socket_addrinfo_lookup()` (`getaddrinfo()`), which reports the address family of every result, so IPv4 literals, IPv6 literals and hostnames are all handled uniformly. This also fixes hostnames that only publish `AAAA` records: an earlier attempt at this chose the family by testing the host for an IPv6 literal, which left every name on `AF_INET`, where `socket_connect()` resolves via `gethostbyname()` and can never see an IPv6 address. IPv6 literals are additionally accepted in bracketed form (`[::1]`), and a host that resolves to nothing now fails with a dedicated `SOCKET_HOST_RESOLUTION_FAILED` error rather than an opaque connect failure.
* `Cassandra\Connection\Socket::connect()`: when a hostname resolved to several addresses, only the first was ever tried, so a dual-stack host whose first address was unreachable failed to connect even with a working second address. Every resolved address is now tried in turn, and the last error is reported if all of them fail.
* `Cassandra\Connection\Socket::connect()`: the connect retry loop re-invoked `socket_connect()` with the configured host, so every `EINTR`-interrupted attempt triggered a fresh DNS lookup. The loop now connects to the already-resolved numeric address.
* `Cassandra\Value\UDT` / `Cassandra\Value\Tuple`: a serialized value carrying fewer fields than its type declares (for example UDT rows written before an `ALTER TYPE ... ADD`) was decoded by reading past the value's declared length into whatever bytes followed, desyncing the row decoder. Decoding now stops at the declared length and null-fills the remaining trailing fields.
* `Cassandra\Request\Request`: in the untyped value path (a raw PHP value bound without type metadata, i.e. `query(..., autoPrepare: false)` or `Batch::appendQuery()`), an `int` outside the 32-bit range was silently truncated by `pack('N')`, and a `DateTime` was encoded as a formatted string that no temporal column accepts. Both now throw a `Cassandra\Exception\RequestException` pointing at `Value\Bigint`/`Value\Timestamp` or prepared statements, instead of sending a wrong value.
* `Cassandra\Value\Duration`: the string parser silently accepted arbitrary garbage. The value-format patterns were unanchored and consist entirely of optional components, so any input (e.g. `"hello world"`, or a typo like `"5x2d"` where `"2d"` was intended) matched an empty region and produced a zero duration instead of an error. The patterns are now anchored and a match must capture at least one component; invalid strings throw a `Cassandra\Exception\ValueException`.
* `Cassandra\Value\Decimal`: constructing a decimal from a PHP float rounded it to an integer (`new Decimal(1.5)` stored `2`, `new Decimal(0.1)` stored `0`), silently corrupting the written value. Floats are now converted to a plain decimal string preserving their exact (shortest round-trip) value, and non-finite floats (`INF`/`NAN`) are rejected.
* `Cassandra\Value\Decimal::fromBinary()`: the scale field was decoded as an unsigned int instead of the signed int32 the protocol defines. A negative scale (legal for Java `BigDecimal` values) was misread as a huge positive scale, and building the padded value string then exhausted memory. The scale is now sign-extended and negative scales decode correctly (`unscaled * 10^-scale`).
* `Cassandra\Value\Decimal::fromBinary()`: decoding expands the value into a plain decimal string whose length grows with the (signed int32) scale, so a crafted or corrupt `decimal` cell as small as five bytes could force an allocation of up to ~2 GiB and exhaust memory — a decode-side denial-of-service reachable from any query returning a `decimal` column. The absolute scale is now bounded and an out-of-range scale is rejected with a `Cassandra\Exception\ValueException`.
* `Cassandra\Value\Decimal`: constructing a decimal from a numeric string that `is_numeric()` accepts but the varint wire encoding cannot express verbatim — scientific notation (`"1e5"`), a leading `"+"`, surrounding whitespace, or a bare-point form (`".5"`) — was accepted at construction but then threw an "invalid varint" error only later, at `getBinary()` (send) time. Such strings are now normalized to a plain decimal string at construction; plain decimal strings keep their explicit scale (trailing zeros like `"1.50"`) verbatim.
* `Cassandra\Request\Request`: values bound to a prepared statement whose key did not exactly match the server-reported bind marker name — including a mere case difference like `['userId' => …]` against the (lowercase-stored) column `userid` — were silently encoded as `null`, potentially overwriting existing data with a tombstone. The same happened for missing positional values. Named values now match bind marker names case-insensitively, and a value that is genuinely missing for a bind marker throws a `Cassandra\Exception\RequestException` instead of writing `null` (an explicit `null` or `Value\NotSet` is still honoured).
* `Cassandra\Request\Request`: with `names_for_values`, bind names taken from the server's prepared-statement metadata were lowercased before being sent back, which corrupted case-sensitive (quoted) identifiers like `"userId"`. Names originating from server metadata are now sent back unchanged; only user-supplied names (non-prepared `QUERY` path) are still lowercased to match unquoted-identifier semantics.
* `Cassandra\Connection\Socket` / `Cassandra\Connection\Stream`: configured receive timeouts were never enforced on the (default) non-blocking read path — the read select blocked without a timeout, and for streams `stream_set_timeout()` has no effect once the stream is non-blocking. A peer that stayed connected but silent could block a synchronous request forever regardless of `SO_RCVTIMEO` / `timeoutInSeconds`. Reads now wait at most for the configured receive timeout and then throw a timeout exception, matching the blocking-I/O behaviour.
* `Cassandra\Connection\Stream` / `Cassandra\Connection\StreamNodeConfig`: configuring `sslOptions` on a host without an explicit `tls://` scheme silently produced a **plaintext** connection, because the ssl context options only take effect on a tls transport. A non-empty `sslOptions` array now enables TLS automatically when the host carries no scheme (an explicit scheme is still respected). The `sslOptions` default changed from an array of verify flags to `[]`; this is behavior-neutral, as the removed defaults match PHP's built-in ssl-context defaults, which continue to apply to any option not set explicitly.
* `Cassandra\Connection\Socket`: a host containing a URL scheme (e.g. `tls://…`, which the socket transport cannot handle) now fails with a clear configuration error pointing at `StreamNodeConfig`, instead of a confusing DNS resolution failure.
* `Cassandra\Response\ResultIterator`: calling `next()` without a preceding `current()` advanced the iteration position but not the row cursor, desynchronizing keys and rows for the rest of the iteration (harmless in a normal `foreach`, which always calls `current()`). `next()` now skips the unread row so position and cursor stay aligned.
* `Cassandra\Value\MapCollection`: a map whose key type decodes to a PHP float or bool (e.g. `map<double, text>`, which is legal CQL) could not be read at all — decoding threw a `Cassandra\Exception\ValueException`. Float keys are now represented as strings (a PHP array would silently truncate them to int) and boolean keys as `1`/`0`.
* `Cassandra\Value\UDT` / `Cassandra\Value\Tuple`: `getBinary()` accessed undeclared fields with an "undefined array key" warning when the supplied value array was missing a field of the type definition. Missing fields are now encoded as `null` cleanly.
* `Cassandra\Request\Prepare::getHash()`: the prepared-statement cache key was built by concatenating query and keyspace without a separator, so distinct `(query, keyspace)` pairs could collide in the cache. The two parts are now delimited.
* `Cassandra\Value\Uuid` / `Cassandra\Value\Timeuuid`: the constructor accepted any string without validation, so a malformed value was silently turned into wrong-length or corrupt binary by `getBinary()` (`pack('H*', …)` coerces non-hex characters to `0`) — a typo'd UUID could be written to the server as 16 bytes of wrong data. The value is now validated at construction — a string that is none of the canonical `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` form (case-insensitive), the compact 32-character undashed hex form, or the raw 16-byte binary form is rejected with a `Cassandra\Exception\ValueException`.
* `Cassandra\Value\Date`: the integer input was documented as "number of days since 1970-01-01", but it is actually the raw `date` wire value — a 32-bit unsigned integer with the epoch centered at `2^31` (so `2^31` is 1970-01-01), matching the [CQL spec](https://cassandra.apache.org/doc/latest/cassandra/developing/cql/types.html#dates). The behavior was already correct; the misleading constructor doc and error message are now corrected.
* `Cassandra\Value\Vector`: fixed two element-framing bugs. `vector<smallint>`/`vector<tinyint>` are variable-length inside vectors ([CASSANDRA-14476](https://issues.apache.org/jira/browse/CASSANDRA-14476)), but `fromStream()` expected no length prefix and misread them; and `getBinary()` wrote the variable-length prefix as a two's-complement varint rather than an unsigned VInt, corrupting elements of 128 bytes or more (e.g. a long `vector<text>`). Both paths now use unsigned-VInt prefixes and agree.
* `Cassandra\Value\Vector`: `getBinary()` serializes exactly `dimensions` elements by positional index, so a value array that was not a `dimensions`-length list was silently mis-encoded — extra elements were dropped without error, and too few or non-`0`-indexed (associative) elements produced "undefined array key" warnings followed by a misleading `ValueFactoryException`. The constructor now rejects a non-list value or an element count that does not match the declared dimensions with a clear `Cassandra\Exception\ValueException`.
* `Cassandra\Request\Request`: when binding named values against prepared-statement metadata, two bind markers reported under the same name collapsed into a single entry, silently sending fewer values than the statement has markers. This is now rejected with a `Cassandra\Exception\RequestException` directing the caller to positional (sequential) binding.
* `Cassandra\Response\Result\SchemaChangeResult` / `Cassandra\Response\Event\SchemaChangeEvent`: schema changes targeting a keyspace (`CREATE`/`ALTER`/`DROP KEYSPACE`) were rejected with "Invalid schema change target", because the `KEYSPACE` target — which carries no `name` field per the protocol spec — was missing from the target dispatch. For results this threw when accessing the schema change data; for events it threw while parsing the response, so a registered event listener could not receive keyspace-level schema change events at all. The event-side `SchemaChangeData::$name` is now nullable, matching the result side, and is `null` for keyspace targets.
* `Cassandra\TypeNameParser`: UDT type strings in the Java class-name format (`org.apache.cassandra.db.marshal.UserType(...)`, received for columns whose type arrives as a custom type, option `0x0000`) were parsed without decoding the hex-encoded UDT name and field names that Cassandra emits in this format. Field names came back as hex strings (`7a6970` instead of `zip`), and — because PHP coerces all-decimal-digit array keys to integers — a field whose hex form happened to contain only digits (e.g. `street` → `737472656574`) made the parser reject the whole type string with a "field keys must be strings" error. Parameter names are now carried as real strings and hex-encoded UDT names and field names are decoded.
* `Cassandra\Response\Response::__toString()`: the re-serialized frame header packed the `ProtocolVersion` enum instance instead of its integer value (emitting a PHP warning and a bogus version byte) and did not set the response direction bit. The version byte is now encoded as `0x80 | version`.
* `Cassandra\Value\Boolean`: a zero-length (empty, non-null) `boolean` cell — legal in Cassandra and distinct from `null` — decoded as `true`. It now decodes as `false`, matching the other drivers.
* `Cassandra\Value\Custom::requiresDefinition()` returned `false` although decoding a custom value requires a `CustomInfo` definition. `ValueFactory::getTypeInfoFromType(Type::CUSTOM)` therefore handed out a bare `SimpleTypeInfo` that later failed inside `Custom::fromBinary()` with a misleading "Invalid type info" error. It now returns `true`, so the factory reports upfront that a custom type needs a definition.

### Performance
* `Cassandra\Connection\NodeImplementation`: while a large response body accumulated across partial reads, the read buffer was rebuilt (fully copied) on every read; it is now appended to in place when nothing has been consumed yet, avoiding quadratic copying.
* Response and value dispatch maps (`Cassandra\Response\Response`, `Result`, `Error`, `Event`, `Cassandra\ValueFactory`, `Cassandra\TypeNameParser`) are now lazily cached instead of being rebuilt on every call.
* `Cassandra\Response\StreamReader::readValue()`: scalar, list and set cells are now decoded directly without allocating an intermediate `Value` object per cell.

### Tests
* Added integration tests for collection updates (incremental `+`/`-`, prepend, indexed assignment, single-element deletion, clearing, TTL, conditional updates, and frozen and nested collections), lightweight transactions, CQL JSON (`INSERT JSON`, `SELECT JSON`, `fromJson()`/`toJson()`, `DEFAULT UNSET`/`DEFAULT NULL`), user-defined functions and aggregates, materialized views, secondary indexes (including `KEYS()`, `ENTRIES()`, `FULL()` and SASI/`LIKE` index targets), authentication (roles, `GRANT`/`REVOKE`, permissions), and further CQL features (tracing, virtual tables, TTL/`WRITETIME`, schema evolution, aggregates, static columns and counter batches).
* Added regression tests for prepared-statement repreparation, asynchronous auto-preparation, `waitForAnyStatement()` and reconnect after `disconnect()`.
* Added integration tests for the ScyllaDB-only `BYPASS CACHE` and `USING TIMEOUT` CQL extensions, asserting both that they work on ScyllaDB and that Apache Cassandra rejects them.
* Added a unit test for the binary encoding of the `serialConsistency` request option.
* Added integration tests for empty (zero-length) values of fixed-length types, asserting that empty values are decoded as `null` and do not desync the row decoder.
* Added regression tests for `vector<smallint>`/`vector<tinyint>` framing and for variable-length vector elements of 128 bytes or more (`vector<text>`).
* Added unit tests for the untyped-path guards rejecting out-of-int32-range integers and `DateTime` bound values.
* Added unit tests for invalid duration strings, decimals built from floats (including the non-finite guard), decimals built from scientific-notation/`+`-signed/bare-point numeric strings (and verbatim preservation of plain decimals), negative decimal scales, and prepared-statement bind value matching (case-insensitive names, exact-case encoding of server-reported names, and the missing-value guard).
* Added unit tests for `Cassandra\Value\Vector` rejecting associative and count-mismatched values, and for the duplicate-named-bind-marker guard.
* Added unit tests for `Cassandra\Value\Uuid` / `Cassandra\Value\Timeuuid` rejecting malformed values (and accepting uppercase hex).
* Added unit tests for keyspace-level schema change results and events, `Response::__toString()` frame-header encoding, hex-encoded (including all-digit-hex) UDT names in Java class-name type strings and the rejection of non-hex ones, and empty `boolean` cells decoding as `false`.
* Added unit tests for the `uuid`/`timeuuid` encode option (`AS_STRING` vs `AS_BINARY`), including nested `list<uuid>` decoding.
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
