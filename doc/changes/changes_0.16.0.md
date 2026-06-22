# 0.16.0 - 2026-06-22

## Summary

## Security Issues

This release fixes vulnerabilities by updating dependencies:

| Dependency | Vulnerability | Affected | Fixed in |
|------------|---------------|----------|----------|
| gitpython | CVE-2026-42215 | 3.1.46 | 3.1.47 |
| gitpython | CVE-2026-42284 | 3.1.46 | 3.1.47 |
| gitpython | CVE-2026-44244 | 3.1.46 | 3.1.49 |
| gitpython | GHSA-mv93-w799-cj2w | 3.1.46 | 3.1.50 |
| idna | PYSEC-2026-215 | 3.11 | 3.15 |
| pip | PYSEC-2026-196 | 26.0.1 | 26.1.2 |
| pip | CVE-2026-3219 | 26.0.1 | 26.1 |
| pip | CVE-2026-6357 | 26.0.1 | 26.1 |
| pytest | CVE-2025-71176 | 7.4.4 | 9.0.3 |
| tornado | CVE-2026-49854 | 6.5.5 | 6.5.6 |
| tornado | CVE-2026-49853 | 6.5.5 | 6.5.6 |
| tornado | CVE-2026-49855 | 6.5.5 | 6.5.6 |
| urllib3 | PYSEC-2026-142 | 2.6.3 | 2.7.0 |
| urllib3 | PYSEC-2026-142 | 2.6.3 | 2.7.0 |
| urllib3 | PYSEC-2026-141 | 2.6.3 | 2.7.0 |

* #148: Fixed vulnerabilities by updated dependencies, increased allowed `pytest` version, and updated to `exasol-toolbox` 7.0.0

## Refactoring

* #151: Updated exasol-toolbox to 8.1.1
* #154: Enabled on-prem integration tests for all supported Python versions.
* #159: Fixed the extensions of uploaded files in integration tests to comply with SaaS rules.
* #160: Moved to the new SLC: template-Exasol-all-python-3.12-release_x64_11.2.0.

## Dependency Updates

### `main`

* Updated dependency `click:8.3.2` to `8.4.1`
* Updated dependency `pyexasol:2.2.0` to `2.2.1`
* Updated dependency `requests:2.33.1` to `2.34.2`

### `dev`

* Updated dependency `exasol-toolbox:6.2.0` to `8.1.1`
* Updated dependency `pytest:7.4.4` to `9.0.3`
* Updated dependency `pytest-exasol-backend:1.4.0` to `1.4.1`
