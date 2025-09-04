Todo
=====

* remove unused exceptions codes
* maybe remove usage of root exception 

* Finish tests
  * test fromStream()
  * test streamreader
  * named values auto-detection
  * Add stress tests for concurrent queryAsync() across many statements, fault‑injection for node failure and automatic reprepare paths, and paging stress with large result sets.

* Refactor test keyspace, truncation and connection handling

* Add to readme: 
  * BATCH does not support names for values
  * QUERY and EXECUTE: namesForValues is auto-detected if not set explcitly
  
* Add examples

* should query options really be nullable?
  