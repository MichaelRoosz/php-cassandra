Todo
=====

* Add tests
* Cleanup exception messages
* Add documentation
* Update README.md
* Types
  + Remove uneeded bin -> int -> bin conversions
  + Add fromValue() and fromMixedValue()
  + Move bin conversions to a single place
* Refactor fetch*() API
  PDOStatement::fetch — Fetches the next row from a result set
  PDOStatement::fetchAll — Fetches the remaining rows from a result set
  PDOStatement::fetchColumn — Returns a single column from the next row of a result set
  PDOStatement::fetchObject — Fetches the next row and returns it as an object
  PDOStatement::columnCount — Returns the number of columns in the result set
  PDOStatement::rowCount — Returns the number of rows affected by the last SQL statement
* Fix keyspace handling
    string(468) "`USE <keyspace>` with prepared statements is considered to be an anti-pattern due to ambiguity in non-qualified table names. Please consider removing instances of `Session#setKeyspace(<keyspace>)`, `Session#execute("USE <keyspace>")` and `cluster.newSession(<keyspace>)` from your code, and always use fully qualified table names (e.g. <keyspace>.<table>). Keyspace used: test_keyspace, statement keyspace: test_keyspace, statement id: 9b621053664255b24423346115d20c50"
* Convert to classes
  + Connection config
  + Errors
  + Events
