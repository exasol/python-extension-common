# Unreleased

This release prepares validation of Script Language Containers (SLC) uploaded to BucketFS as a file archive, e.g. with extension `tar.gz`.

Validation is implemented by
* activating the uploaded SLC,
* running a UDF inside it,
* and making the UDF check for a specific file having been extracted from the SLC archive to be available on each node of the databasecluster.

Addtionally this release refactors the existing CLI tests for the `LanguageContainerDeployer` which were integration tests involving the whole chain from the CLI down to the API, starting a database and uploading and activating SLCs.

The existing integration tests have been split into
* either unit tests just verifiying that the CLI options are passed to the API
* or ordinary integration tests not using the CLI.

This enables faster and more robust tests for the pure CLI-related features, faster turnaounds during development, and separation of concerns.

## Features

* #50: Created new implementation `ExtractValidator` for validating extraction of
* #49: Integrated new `ExtractValidator` into `LanguageContainerDeployer`

# Refactoring

* #51: Split CLI integration tests
