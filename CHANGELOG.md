## v1.0.0

todo: some intro

### Added
* Updated Asynchronous API, now providing additional helper methods ( `waitForStatements`, `waitForAnyStatement`) and supporting non-blocking response processing ( `drainAvailableResponses`, `tryResolveStatement`, `tryResolveStatements`)
* Events subsystem: blocking (`waitForNextEvent`) and non-blocking event polling (`tryReadNextEvent`)
* New `Value` capabilities including `Vector` data type support
* Support for configuring value encoding for data types with multiple encodings
* Warnings listener interface and registration
* Round-robin and random node selection strategies

### Changed
* Prepared statements: auto-prepare for typed execution, names-for-values auto-detection, re-prepare on UNPREPARED, metadata caching; helpers `executeAll`, pagination helpers

### Fixed
* Numerous stability and correctness fixes across execute/query flows and pagination edge cases
* Richer server exception context (consistency levels, required/received counts, write types)

### Documentation
* Comprehensive README: quick start, configuration reference, async/event APIs, migration guide, error handling.

## v0.8.1
* Allow types Date, Time and Timestamp to be created from string values

## v0.8.0
* Improve API for prepared statements
* Fix a bug in execute call
* Fix phpdoc for paging_state
* Fix version calculation for server packets
* Replace SplFixedArray with array
