# Unreleased

## Summary

We updated the `poetry.lock` file in this repository to fix the following security vulnerabilities: 

* CVE-2025-68146: `TOCTOU` symlink vulnerability in SoftFileLock (affects `filelock` 3.20.1, `virtualenv` 20.35.3)
* CVE-2026-21441: Decompression bomb vulnerability (affects `urllib3` 2.6.2)
* CVE-2025-69277: Improper elyptic curve point calculation vulnerability in `libsodium` (affects `pynacl` 1.6.0)

Please note that the situation on the user's machine differs since they have their own lock file.

## Security

* Security updates to fix vulnerabilities listed above

## Refactorings

* #127: Refactored class `ParameterFormatters` and docstrings
