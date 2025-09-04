Todo
=====

* Rename value-holder classes
  * Cassandra\Type\* → Cassandra\Value\*; TypeBase → ValueBase; TypeFactory → ValueFactory; Type\NotSet → Value\NotSet.

* better handling for "not set" > is it really possible to be returned?

* remove unused exceptions codes
* maybe remove usage of root exception 

* Finish tests
  * test fromStream()
  * test streamreader
  * named values auto-detection
  * Add stress tests for concurrent queryAsync() across many statements, fault‑injection for node failure and automatic reprepare paths, and paging stress with large result sets.

* Add to readme: 
  * BATCH does not support names for values
  * QUERY and EXECUTE: namesForValues is auto-detected if not set explcitly
  
* Add examples
