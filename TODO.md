# Todo for the next release

## Features

## Tests
* Finish tests
  * value encode options
  * test fromStream()
  * test streamreader
  * named values auto-detection
  * Add stress tests for concurrent queryAsync() across many statements, fault‑injection for node failure and automatic reprepare paths, and paging stress with large result sets.


## Documentation
* Add to readme:
  * BATCH does not support names for values
  * QUERY and EXECUTE: namesForValues is auto-detected if not set explcitly
  * Always use fully-qualified tables names (with keyspace) for prepare requests 
* Add examples

## Planned for future releases
* Token-aware Routing
* Datacenter-aware Load Balancing
