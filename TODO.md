# Backlog

Open items for later releases. New findings go here unless they block a release under the rules in `RELEASING.md`.

## Features
* Relevant ScyllaDB protocol extensions from `doc/scylladb-protocol-extensions.md`: LWT metadata mark, rate-limit error, then shard awareness and tablets
* Token-aware routing
* Datacenter-aware load balancing

## Compared with Guzzle and Monolog
* Logging and metrics hooks (PSR-3 or a listener), so operators can see requests, timeouts and reconnects
* An optional retry policy for idempotent statements, like Guzzle's retry middleware

## Performance
* A decode fast path for `time`, which still goes through the value object and validates its range there

## Hardening
* `Lz4Extension::decompressBlock()`: with an error handler that ignores `@`, corrupt LZ4 from a node surfaces the extension's warning as that handler's exception instead of falling back to the PHP decoder
* `Request\AuthResponse`: add `__debugInfo()`; a live connection does not retain it, so only a hand-built one can show its token in a dump
