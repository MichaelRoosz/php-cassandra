# Upgrading

## From 1.x to 2.0

### Likely to affect you

* A slow query now raises `RequestTimeoutException` instead of a transport exception. It is deliberately not a `NodeException`, so `catch (NodeException)` no longer covers it. The connection stays open.
* Every request now has a 30s timeout (`ConnectionOptions::$requestTimeoutInSeconds`). Raise it for slow operations such as `TRUNCATE`. See [Timeouts](README.md#timeouts).
* `waitForNextEvent()`, `waitForNextResponse()` and `waitForAnyStatement()` can return `null` when given a timeout. `waitForNextEvent()` no longer fails on a quiet cluster.
* `setKeyspace()` takes the name exactly as given: a keyspace created unquoted as `MyKs` is `myks`. `setKeyspace('')` on a connected v3/v4 connection now raises, because that keyspace cannot be left; `disconnect()` first.
* `StreamNodeConfig::$persistent` was removed, which moves `$sslOptions` up one position. Positional callers must adjust; named arguments are unaffected.
* `getResponseForStatement()` raises a server error instead of returning the `Error` frame, as `syncRequest()` does.
* A `Timestamp` string without a UTC offset now means UTC, not the default timezone. `Time::asDateTime()` returns UTC too, like `Date` and `Timestamp`.
* On v5, hand-built requests sent with `syncRequest()`/`asyncRequest()` now run against the connection's keyspace.

### New validation that raises instead of misbehaving

* Connection and request options: a non-positive request timeout or heartbeat setting; a negative `preparedResultCacheSize` or `maxOrphanedStreams`; a `pageSize` or `nowInSeconds` outside signed 32 bits; an empty node list.
* `waitFor…()` with a `NAN` timeout.
* `Varint`/`Decimal` magnitudes above 4096 digits, which were a decode-side denial of service on the pure-PHP calculator.
* `Inet` addresses at construction, `timeuuid` values that are not version 1, and null elements in collections.
* Default timestamps the protocol forbids, and quoted numeric bind markers such as `:"0"` without an explicit `namesForValues`.

### Only if you use the low-level API

* `fromBinary()` and `ValueFactory::getValueObjectFromBinary()` are nullable: an empty value of `bigint`, `boolean`, `counter`, `decimal`, `double`, `float`, `inet`, `int`, `timestamp`, `timeuuid`, `uuid` or `varint` returns `null`. `Varint::fromBinary('')` no longer returns `0`.
* `Request::getStream()` and `Frame::getStream()` return `?int`; `Request::__construct()` takes `?int $stream`. Encoding a request without an id throws.
* `ReadFailureContext::$dataPresent` is a `bool`. `SchemaChangeData::$name` is nullable: keyspace-level changes have no name.
* `Request\AuthResponse` takes the SASL token rather than a username and password.
* `Statement::__construct()` gained parameters, and `StreamReader` is `final`.
* Custom transports: `Node::read()` and `readAvailableDataFromSource()` take `?float $readDeadline` (a point on `Connection\Clock::now()`, or `Node::DO_NOT_WAIT`) instead of `bool $waitForData`; `Node::readAvailableData()` was removed; `Node::getReceivedByteCount()` was added, and `NodeImplementation` implements it.
* Removed: `ProgressiveStreamReader`, `Opcode::RESPONSE_CREDENTIALS`, and the exception codes `CONNECTION_AUTH_INVALID_NODE_IMPLEMENTATION`, `CONNECTION_READY_INVALID_NODE_IMPLEMENTATION`, `RESPONSE_EVENT_SCHEMA_CHANGE_UNEXPECTED_TARGET_VALUE`, `RESPONSE_PSR_GET_DATA_NOT_SUPPORTED`, `RESPONSE_PSR_SOURCE_NOT_SET`, `RESPONSE_SCHEMA_CHANGE_UNEXPECTED_TARGET_VALUE`, `RESPONSE_SR_UNPACK_VALUE_LENGTH_FAIL`, `VALUE_INET_PARSE_FAIL` and `VALUE_INET_TO_BINARY_FAILED`, and `CONNECTION_UNKNOWN_ERROR_CODE`, `CONNECTION_UNKNOWN_EVENT_TYPE` and `CONNECTION_UNKNOWN_RESULT_KIND`, as well as `NODE_DECOMPRESSOR_NOT_INITIALIZED` and `NODE_PAYLOAD_EXCEEDS_MAX`, which could not occur: an unknown code, event or kind raises a `ResponseException` while the response is read.
* Deprecated: `Connection::getNode()` and `ProtocolVersion::PREFRED_ORDER`, replaced by `PREFERRED_ORDER`.
