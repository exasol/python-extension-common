# 0.13.1 - 2026-04-02

## Summary

We updated the dependencies in the `poetry.lock` file in this repository to fix the following security vulnerabilities: 

| Name         | Version | ID                  | Fix Versions |
|--------------|---------|---------------------|--------------|
| black        | 25.12.0 | CVE-2026-32274      | 26.3.1       |
| cryptography | 46.0.5  | CVE-2026-34073      | 46.0.6       |
| pyasn1       | 0.6.2   | CVE-2026-30922      | 0.6.3        |
| pygments     | 2.19.2  | CVE-2026-4539       | 2.20.0       |
| requests     | 2.32.5  | CVE-2026-25645      | 2.33.0       |
| tornado      | 6.5.4   | GHSA-78cv-mqj4-43f7 | 6.5.5        |
| tornado      | 6.5.4   | CVE-2026-31958      | 6.5.5        |

Please note that the situation on the user's machine differs since they have their own lock file.

We also updated versions in the GitHub actions. Users are not affected by this.

## Security

* #136: Security updates to fix vulnerabilities listed above

## Refactorings

* #127: Refactored class `ParameterFormatters` and docstrings

## Dependency Updates

### `main`

* Updated dependency `exasol-bucketfs:2.1.0` to `2.2.0`
* Updated dependency `exasol-saas-api:2.8.0` to `2.9.0`
* Updated dependency `pyexasol:2.0.0` to `2.2.0`
* Updated dependency `requests:2.32.5` to `2.33.1`

### `dev`

* Updated dependency `exasol-toolbox:6.0.0` to `6.1.1`
* Updated dependency `pytest-exasol-backend:1.3.0` to `1.4.0`
