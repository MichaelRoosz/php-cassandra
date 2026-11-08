# Contributing

## Setup

```bash
git clone https://github.com/MichaelRoosz/php-cassandra.git && cd php-cassandra
composer install
```

## Before you open a pull request

```bash
composer lint                # composer lint-fix applies the fixes
composer phpstan && composer phpstan:tests
composer psalm
composer test:unit
composer fuzz
composer check-readme
```

Integration tests need Docker, and take a few minutes per server:

```bash
composer test:integration
composer test:integration_auth
composer test:integration_scylladb
```

## Conventions

- Add a regression test with every bug fix.
- The library only throws exceptions from `Cassandra\Exception`. New failure modes get their own `ExceptionCode`.
- PHPStan and Psalm run at their strictest levels with no baseline, so fix findings rather than suppressing them.
- Comments explain why, not what.
- Do not break the public API outside a major version. Classes marked `@internal` are exempt.

The full conventions are in `AGENTS.md`. Report vulnerabilities as described in `SECURITY.md`, not in an issue.
