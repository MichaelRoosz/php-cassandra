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

## Comments

- Comment why, not what. The code already states what it does; a comment earns its place only where the reason cannot be recovered from it — a non-obvious language behaviour, an invariant, the arithmetic justifying a bound, or a deliberate trade-off.
- Keep them compact, as a professional library does: one or two lines at the point of difficulty, and a docblock of a few lines at most. Prose that runs to paragraphs belongs in the class docblock or in `doc/`, not beside a statement.
- Prefer expressive code to commentary. A named constant, a well-named helper, or an early return usually removes the need for the comment entirely.
- Use a docblock for what a caller must know (contract, `@throws`, units, ownership) and inline `//` for reasoning that only concerns someone editing the body.
- Do not restate the signature. Document a parameter only where its contract is narrower or less obvious than its declared type.
- Do not narrate change history in source comments — git records it. Regression tests are the exception: naming the defect being defended is the point of the test.
- Keep comments true. Update or delete them along with the code they describe; a stale comment is worse than none.
- No commented-out code, no decorative banners, no TODOs without an issue reference.

## PHPDoc

- Keep PHPDoc types as narrow as the intended contract permits. Do not widen a public parameter or return type merely to make defensive runtime validation visible to a static analyzer; isolate that validation behind an internal helper with the broader input type instead.
