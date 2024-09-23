# 0.5.0 - 2024-09-19

This release prepares validation of Script Language Containers (SLC) uploaded to BucketFS as a file archive, e.g. with extension `tar.gz`.

Validation is implemented by
* activating the uploaded SLC,
* running a UDF inside it,
* and making the UDF check for a specific file having been extracted from the SLC archive to be available on each node of the databasecluster.

Additionally, this release refactors the existing CLI tests for the `LanguageContainerDeployer` which were integration tests involving the whole chain from the CLI down to the API, starting a database and uploading and activating SLCs.

The existing integration tests have been split into
* either unit tests just verifying that the CLI options are passed to the API
* or ordinary integration tests not using the CLI.

This enables faster and more robust tests for the pure CLI-related features, faster turnarounds during development, and separation of concerns.

The integration tests now use the pytest plugin `pytest-exasol-backend`.

## Features

* #50: Created new implementation `ExtractValidator` for validating extraction of
* #49: Integrated new `ExtractValidator` into `LanguageContainerDeployer`

# Refactoring

* #51: Split CLI integration tests
* #63: Removed the language_alias parameter from the LanguageContainerBuilder.

## Bug Fixing

* #60: Fixed handling pip requirements when creating an SLC.
* #58: Fixed the bug in language_container_builder.find_path_backwards.
* #36: Added SAAS_HOST environment variable to the user guide.
* #35: Restoring pre-existing schema in the temp_schema module.
