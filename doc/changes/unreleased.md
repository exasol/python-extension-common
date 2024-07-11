# Unreleased

## Security Issues

* Fixed vulnerabilities by updating dependencies
  * Vulnerability CVE-2024-21503 in transitive dependency via `exasol-toolbox` to `black` in versions below `24.3.0`
  * Vulnerability CVE-2024-35195 in dependency `requests` in versions below `2.32.0` caused by requests `Session` object not verifying requests after making first request with `verify=False`
