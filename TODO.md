Todo
=====

* Add tests
* Cleanup exception messages
* Add documentation
* Update README.md
* Types
  + Remove uneeded bin -> int -> bin conversions

* Refactor fetch*() API
  PDOStatement::fetch — Fetches the next row from a result set
  PDOStatement::fetchAll — Fetches the remaining rows from a result set
  PDOStatement::fetchColumn — Returns a single column from the next row of a result set
  PDOStatement::fetchObject — Fetches the next row and returns it as an object
  PDOStatement::columnCount — Returns the number of columns in the result set
  PDOStatement::rowCount — Returns the number of rows affected by the last SQL statement

* Implement automatically re-connecting to nodes and load-balancing
