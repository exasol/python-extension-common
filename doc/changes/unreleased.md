# Unreleased

## Summary

## Bug Fixes

* #168: Fixed `get_cli_arg`/`kwargs_to_cli_args` raising `NoSuchOption` for secret
  option values starting with `-`/`--` (e.g. SaaS/DB credentials and ids).
* #173: Fixed `secret_callback`'s env-var fallback using a hyphenated name (e.g.
  `DB-PASSWORD`) instead of the documented `DB_PASSWORD`.

## Refactorings

* #183: Removed support for Python 3.10 and updated exasol-toolbox to 12.0.0