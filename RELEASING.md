# Releasing

A release is finished when the gates below pass, not when a review stops finding things. Reviewing 30k lines always finds something, so "search for errors" is never a release task on its own.

## Scope rules

1. **The scope is frozen when the release branch starts.** New work enters only through rule 2.
2. **A finding blocks the release only if all of these hold:**
   - It has a failing test or a reproduction through the public API.
   - The input is one a real caller or a real server produces, not a hand-crafted frame "no node sends".
   - It causes one of: silently wrong data, a crash or hang, a native exception crossing the library boundary, a security problem, or documentation that is false.
3. **An API decision that would be a breaking change later** is also handled now, even without a bug.
4. **Everything else goes to the backlog in `TODO.md`** and is not fixed in this release. That includes extra hardening, refactors, wording, additive features and theoretical edge cases.
5. **Fixes are not re-audited.** A fix gets a regression test and a review of its own diff. Gate 2 is not reopened for the rest of the code.

## Gate 1: mechanical checks

All must pass. These replace opinion wherever a tool can decide.

| Check | Command |
|---|---|
| Style | `composer lint` |
| Static analysis | `composer phpstan`, `composer phpstan:tests`, `composer phpstan:32bit`, `composer psalm` |
| Exception codes | `composer find-unused-exceptions` |
| Unit tests | `composer test:unit` |
| Whitespace | `git diff --check` |
| Parser fuzzing | `composer fuzz`: only `Cassandra\Exception` may escape |
| README | `composer check-readme`: blocks parse and run, referenced API and links exist |
| Breaking changes | `php scripts/api_diff.php <previous tag>`: every entry is in the changelog |
| Upgrade notes | `UPGRADE.md` covers every breaking change |
| Package | `composer validate --strict`, driver version in `src/ReleaseConstants.php`, changelog heading |

## Gate 2: bounded audit

One pass per row, once per release. A row is ticked when read against its question. Findings go through rule 2.

| Surface | Question |
|---|---|
| Stream ids | Can an id be reused while a late answer may still arrive? |
| Deadlines | Can any wait block past its stated bound? |
| Reconnect | Are in-flight statements, keyspace and prepared cache correct afterwards? |
| Repreparation | Do values, options and timeouts survive a re-send? |
| Value round trip | Does every CQL type read back what was written? |
| Credentials | Can a password or auth token reach an exception, log or dump? |
| Transport | Do partial reads and writes, EOF and TLS behave on both transports? |

## Gate 3: real servers

Slow, so run it after the code is frozen, then re-run only the affected test classes after a fix.

```sh
composer test:integration        # Cassandra, socket and stream
composer test:integration_auth   # PasswordAuthenticator
composer test:integration_scylladb
```

## Publish

Move `## Unreleased` to the version heading, tag, and let CI confirm the PHP 8.1 to 8.5 and 32-bit matrix.
