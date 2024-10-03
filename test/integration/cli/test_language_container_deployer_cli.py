from typing import Any
from urllib.parse import urlparse
import pytest
import click
from click.testing import CliRunner

from exasol.python_extension_common.cli.std_options import (
    StdTags, StdParams, ParameterFormatters, select_std_options)
from exasol.python_extension_common.connections.pyexasol_connection import (
    open_pyexasol_connection)
from exasol.python_extension_common.cli.language_container_deployer_cli import (
    LanguageContainerDeployerCli)
from test.utils.db_utils import (assert_udf_running, open_schema)

CONTAINER_URL_ARG = 'container_url'
CONTAINER_NAME_ARG = 'container_name'


@pytest.fixture
def onprem_params(exasol_config,
                  bucketfs_config,
                  language_alias) -> dict[str, Any]:

    parsed_url = urlparse(bucketfs_config.url)
    host, port = parsed_url.netloc.split(":")
    return {
        StdParams.dsn.name: f'{exasol_config.host}:{exasol_config.port}',
        StdParams.db_user.name: exasol_config.username,
        StdParams.db_password.name: exasol_config.password,
        StdParams.bucketfs_host.name: host,
        StdParams.bucketfs_port.name: port,
        StdParams.bucketfs_use_https.name: parsed_url.scheme.lower() == 'https',
        StdParams.bucketfs_user.name: bucketfs_config.username,
        StdParams.bucketfs_password.name: bucketfs_config.password,
        StdParams.bucketfs_name.name: 'bfsdefault',
        StdParams.bucket.name: 'default',
        StdParams.use_ssl_cert_validation.name: False,
        StdParams.path_in_bucket.name: 'container',
        StdParams.language_alias.name: language_alias
    }


def make_args_string(**kwargs) -> str:
    return ' '.join(f'--{k} "{v}"' for k, v in kwargs.items())


def test_slc_deployer_cli_onprem_url(use_onprem,
                                     backend_aware_onprem_database,
                                     container_version,
                                     container_name,
                                     container_url_formatter,
                                     language_alias,
                                     db_schema,
                                     onprem_params):
    if not use_onprem:
        pytest.skip("The test is not configured to use ITDE.")

    ver_formatter = ParameterFormatters()
    ver_formatter.set_formatter(CONTAINER_URL_ARG, container_url_formatter)
    ver_formatter.set_formatter(CONTAINER_NAME_ARG, container_name)

    opts = select_std_options(
        [StdTags.DB | StdTags.ONPREM, StdTags.BFS | StdTags.ONPREM, StdTags.SLC],
        formatters={StdParams.version: ver_formatter})
    cli_callback = LanguageContainerDeployerCli(
        container_url_arg=CONTAINER_URL_ARG,
        container_name_arg=CONTAINER_NAME_ARG)
    extra_params = {StdParams.version.name: container_version}
    args = make_args_string(**onprem_params, **extra_params)

    cmd = click.Command('deploy_slc', params=opts, callback=cli_callback)
    runner = CliRunner()
    runner.invoke(cmd, args=args)

    with open_pyexasol_connection(**onprem_params) as conn:
        open_schema(conn, db_schema)
        assert_udf_running(conn, language_alias, db_schema)


def test_slc_deployer_cli_onprem_file(use_onprem,
                                      backend_aware_onprem_database,
                                      container_path,
                                      language_alias,
                                      db_schema,
                                      onprem_params):
    if not use_onprem:
        pytest.skip("The test is not configured to use ITDE.")

    opts = select_std_options(
        [StdTags.DB | StdTags.ONPREM, StdTags.BFS | StdTags.ONPREM, StdTags.SLC])
    cli_callback = LanguageContainerDeployerCli()
    extra_params = {StdParams.container_file.name: container_path}
    args = make_args_string(**onprem_params, **extra_params)

    cmd = click.Command('deploy_slc', params=opts, callback=cli_callback)
    runner = CliRunner()
    runner.invoke(cmd, args=args)

    with open_pyexasol_connection(**onprem_params) as conn:
        open_schema(conn, db_schema)
        assert_udf_running(conn, language_alias, db_schema)
