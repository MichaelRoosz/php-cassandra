Todo
=====

## Code

## Tests
* Refactor test keyspace, truncation and connection handling
* Finish tests
  * test fromStream()
  * test streamreader
  * named values auto-detection
  * Add stress tests for concurrent queryAsync() across many statements, fault‑injection for node failure and automatic reprepare paths, and paging stress with large result sets.

## Doc
* Add to readme: 
  * BATCH does not support names for values
  * QUERY and EXECUTE: namesForValues is auto-detected if not set explcitly
* Add examples
