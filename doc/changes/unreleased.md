# Unreleased

Ignoring vulnerability CVE-2024-35195 in dependency `requests` in versions below `2.32.0` caused by requests `Session` object not verifying requests after making first request with `verify=False` as `requests` in version `2.32.0` and higher is incompatible with docker-compose.

## Security Issues

* Fixed vulnerabilities by updating dependencies
  * Vulnerability CVE-2024-21503 in transitive dependency via `exasol-toolbox` to `black` in versions below `24.3.0`
