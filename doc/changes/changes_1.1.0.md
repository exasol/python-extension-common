# 1.1.0 - 2026-10-29

## Summary

Bug fixes, base SLC image and dependency updates

## Security Issues

This release fixes vulnerabilities by updating dependencies:

| Dependency | Vulnerability | Affected | Fixed in |
|------------|---------------|----------|----------|
| black | CVE-2026-114134 | 26.5.1 | 26.10.1 |
| certifi | PYSEC-2026-4320 | 2026.7.22 | 2026.10.18 |
| certifi | PYSEC-2026-4320 | 2026.7.22 | 2026.10.19 |
| cryptography | PYSEC-2026-3552 | 49.0.0 | 50.0.0 |
| filelock | CVE-2026-112907 | 3.32.0 | 4.2.1 |
| filelock | CVE-2026-112908 | 3.32.0 | 4.2.1 |
| filelock | CVE-2026-112909 | 3.32.0 | 4.2.1 |
| filelock | CVE-2026-112910 | 3.32.0 | 4.2.1 |
| filelock | CVE-2026-112911 | 3.32.0 | 4.2.1 |
| filelock | CVE-2026-112913 | 3.32.0 | 4.2.1 |
| filelock | CVE-2026-112915 | 3.32.0 | 4.2.1 |
| filelock | CVE-2026-112935 | 3.32.0 | 4.2.1 |
| filelock | CVE-2026-112936 | 3.32.0 | 4.0.9 |
| filelock | CVE-2026-112938 | 3.32.0 | 4.2.1 |
| filelock | CVE-2026-112939 | 3.32.0 | 4.2.1 |
| filelock | CVE-2026-112940 | 3.32.0 | 4.2.1 |
| gitpython | CVE-2026-110855 | 3.1.57 | 3.2.0 |
| gitpython | CVE-2026-110856 | 3.1.57 | 3.2.0 |
| gitpython | CVE-2026-110857 | 3.1.57 | 3.2.0 |
| gitpython | CVE-2026-110858 | 3.1.57 | 3.2.0 |
| gitpython | CVE-2026-110859 | 3.1.57 | 3.2.0 |
| gitpython | CVE-2026-114145 | 3.1.57 | 3.2.1 |
| gitpython | PYSEC-2026-3783 | 3.1.57 | 3.1.58 |
| gitpython | PYSEC-2026-3784 | 3.1.57 | 3.1.58 |
| gitpython | PYSEC-2026-3785 | 3.1.57 | 3.1.59 |
| gitpython | PYSEC-2026-3786 | 3.1.57 | 3.1.59 |
| gitpython | PYSEC-2026-3787 | 3.1.57 | 3.1.59 |
| gitpython | PYSEC-2026-3788 | 3.1.57 | 3.1.59 |
| gitpython | PYSEC-2026-3837 | 3.1.57 | 3.1.59 |
| gitpython | PYSEC-2026-3838 | 3.1.57 | 3.1.58 |
| gitpython | PYSEC-2026-3840 | 3.1.57 | 3.1.58 |
| gitpython | PYSEC-2026-3841 | 3.1.57 | 3.1.58 |
| gitpython | PYSEC-2026-3843 | 3.1.57 | 3.1.58 |
| gitpython | PYSEC-2026-3982 | 3.1.57 | 3.1.60 |
| gitpython | PYSEC-2026-3984 | 3.1.57 | 3.1.60 |
| msgpack | CVE-2026-114613 | 1.2.1 | 1.2.3 |
| pip | PYSEC-2026-3721 | 26.1.2 | 26.2 |
| pip | PYSEC-2026-3721 | 26.1.2 | 26.2.0 |
| pylint | CVE-2026-114608 | 4.0.6 | 4.0.9 |
| tornado | CVE-2026-115927 | 6.5.7 | 6.6 |
| tornado | CVE-2026-116577 | 6.5.7 | 6.6 |
| tornado | CVE-2026-116578 | 6.5.7 | 6.6 |
| tornado | GHSA-3hv7-mjh2-fv65 | 6.5.7 | 6.5.9 |
| tornado | GHSA-8423-8fgw-73vq | 6.5.7 | 6.5.8 |
| tornado | GHSA-c2m8-h5v5-343r | 6.5.7 | 6.5.9 |
| tornado | GHSA-chx6-46f5-w4vp | 6.5.7 | 6.5.9 |
| tornado | GHSA-wwv5-g3v4-889x | 6.5.7 | 6.5.8 |
| tornado | PYSEC-2026-3928 | 6.5.7 | 6.5.8 |
| urllib3 | PYSEC-2026-4175 | 2.7.0 | 2.8.0 |
| urllib3 | PYSEC-2026-4176 | 2.7.0 | 2.8.0 |
| urllib3 | PYSEC-2026-4177 | 2.7.0 | 2.8.0 |
| virtualenv | CVE-2026-113714 | 21.7.0 | 21.14.9 |
| virtualenv | CVE-2026-113715 | 21.7.0 | 21.14.9 |
| virtualenv | CVE-2026-113717 | 21.7.0 | 21.14.9 |
| virtualenv | CVE-2026-113718 | 21.7.0 | 21.14.9 |
| virtualenv | CVE-2026-113720 | 21.7.0 | 21.14.9 |
| virtualenv | CVE-2026-113722 | 21.7.0 | 21.14.9 |
| virtualenv | CVE-2026-113723 | 21.7.0 | 21.14.9 |
| virtualenv | CVE-2026-113725 | 21.7.0 | 21.14.7 |
| virtualenv | CVE-2026-113726 | 21.7.0 | 21.14.4 |
| virtualenv | CVE-2026-113732 | 21.7.0 | 21.14.2 |
| virtualenv | CVE-2026-113776 | 21.7.0 | 21.14.2 |
| virtualenv | PYSEC-2026-4011 | 21.7.0 | 21.7.12 |
| virtualenv | PYSEC-2026-4012 | 21.7.0 | 21.7.11 |
| virtualenv | PYSEC-2026-4013 | 21.7.0 | 21.7.13 |
| virtualenv | PYSEC-2026-4014 | 21.7.0 | 21.7.12 |

## Bug Fixes

* #168: Fixed `get_cli_arg`/`kwargs_to_cli_args` raising `NoSuchOption` for secret
  option values starting with `-`/`--` (e.g. SaaS/DB credentials and ids).
* #173: Fixed `secret_callback`'s env-var fallback using a hyphenated name (e.g.
  `DB-PASSWORD`) instead of the documented `DB_PASSWORD`.

## Refactorings

* #183: Removed support for Python 3.10 and updated exasol-toolbox to 12.0.0

## Dependency Updates

### `main`

* Updated dependency `click:8.4.2` to `8.5.0`
* Updated dependency `exasol-bucketfs:2.2.0` to `2.3.1`
* Updated dependency `exasol-script-languages-container-tool:4.2.0` to `4.4.0`
* Updated dependency `pyexasol:2.3.0` to `2.4.1`

### `dev`

* Updated dependency `exasol-script-languages-container-tool:4.2.0` to `4.4.0`
* Updated dependency `exasol-toolbox:10.4.0` to `12.0.0`
* Updated dependency `pytest:9.1.1` to `9.2.0`

### `slc`

* Updated base image `exasol/script-language-container:template-Exasol-all-python-3.12-release_x64_12.0.1`
