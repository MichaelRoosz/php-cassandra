# Repository instructions

## Exceptions

- Library code must expose only exceptions from `Cassandra\\Exception`. Never deliberately throw a native PHP exception, and never allow a native exception or `Error` raised by an implementation detail to cross the library boundary. Catch the most specific exception that catches all exceptions being thrown at native/API boundaries and wrap it in the most specific project exception, preserving it as `previous`.
- Do not use a native exception as internal control flow. Perform validation directly and throw the appropriate project exception.
- Every new failure mode must have a dedicated entry in `Cassandra\\Exception\\ExceptionCode` unless an existing code describes exactly the same failure.
- Whenever adding one or more exception codes, update the `Next free code` comment at the top of `src/Exception/ExceptionCode.php` to the next unused integer. Keep all enum values globally unique.
- Custom implementations supplied through extension points such as `NodeSelector`, `NodeConfig`, and `IoNode` are responsible for honoring the same exception contract: they must throw an appropriate exception from `Cassandra\\Exception` themselves. Library code is not required to translate native exceptions or `Error` instances thrown by those custom implementations.

## Verification

- Add regression tests for every bug fix.
- Run the focused PHPUnit tests, PHPStan, Psalm, PHP CS Fixer, and `git diff --check` before handing off changes. In restricted environments, run tools sequentially when their default parallel workers require local sockets.

## PHPDoc

- Keep PHPDoc types as narrow as the intended contract permits. Do not widen a public parameter or return type merely to make defensive runtime validation visible to a static analyzer; isolate that validation behind an internal helper with the broader input type instead.
