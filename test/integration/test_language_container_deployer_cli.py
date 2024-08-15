from typing import Optional, Callable
from contextlib import ExitStack

import pytest
from urllib.parse import urlparse
from click.testing import CliRunner
from pyexasol import ExaConnection, ExaConnectionFailedError
from exasol.pytest_itde import config

from test.utils.revert_language_settings import revert_language_settings
from test.utils.db_utils import (create_schema, assert_udf_running)


TEST_SCHEMA = "PEC_DEPLOYER_TESTS_CLI"
TEST_LANGUAGE_ALIAS = "PYTHON3_PEC_TESTS_CLI"


def call_language_definition_deployer_cli(func,
                                          exasol_config: config.Exasol,
                                          bucketfs_config: config.BucketFs,
                                          language_alias: str,
                                          container_path: Optional[str] = None,
                                          version: Optional[str] = None,
                                          use_ssl_cert_validation: bool = False):
    parsed_url = urlparse(bucketfs_config.url)
    args_list = [
        "language-container",
        "--bucketfs-name", "bfsdefault",
        "--bucketfs-host", parsed_url.hostname,
        "--bucketfs-port", parsed_url.port,
        "--bucketfs-use-https", False,
        "--bucketfs-user", bucketfs_config.username,
        "--bucketfs-password", bucketfs_config.password,
        "--bucket", "default",
        "--path-in-bucket", "container",
        "--dsn", f"{exasol_config.host}:{exasol_config.port}",
        "--db-user", exasol_config.username,
        "--db-pass", exasol_config.password,
        "--language-alias", language_alias
    ]
    if use_ssl_cert_validation:
        args_list += [
            "--use-ssl-cert-validation"
        ]
    else:
        args_list += [
            "--no-use-ssl-cert-validation"
        ]
    if version:
        args_list += [
            "--version", version,
        ]
    if container_path:
        args_list += [
            "--container-file", container_path,
        ]
    runner = CliRunner()
    result = runner.invoke(func, args_list)
    return result
