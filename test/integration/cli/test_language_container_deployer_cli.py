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
from test.utils.db_utils import (assert_udf_running, create_schema)

CONTAINER_URL_ARG = 'container_url'
CONTAINER_NAME_ARG = 'container_name'


@pytest.fixture(scope='session')
def onprem_cli_args(backend_aware_onprem_database,
                    exasol_config,
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
    }


@pytest.fixture(scope='session')
def saas_cli_args(saas_host,
                  saas_pat,
                  saas_account_id,
                  backend_aware_saas_database_id,
                  ) -> dict[str, Any]:
    return {
        StdParams.saas_url.name: saas_host,
        StdParams.saas_account_id.name: saas_account_id,
        StdParams.saas_database_id.name: backend_aware_saas_database_id,
        StdParams.saas_token.name: saas_pat,
    }


@pytest.fixture(scope='session')
def slc_cli_args(language_alias) -> dict[str, Any]:
    return {
        StdParams.alter_system: True,
        StdParams.allow_override: True,
        StdParams.wait_for_completion: True,
        StdParams.path_in_bucket.name: 'container',
        StdParams.language_alias.name: language_alias
    }


@pytest.fixture
def deploy_command(container_name,
                   container_url_formatter) -> click.Command:
    ver_formatter = ParameterFormatters()
    ver_formatter.set_formatter(CONTAINER_URL_ARG, container_url_formatter)
    ver_formatter.set_formatter(CONTAINER_NAME_ARG, container_name)

    opts = select_std_options(
        [StdTags.DB | StdTags.ONPREM, StdTags.BFS | StdTags.ONPREM, StdTags.SLC],
        formatters={StdParams.version: ver_formatter})
    cli_callback = LanguageContainerDeployerCli(
        container_url_arg=CONTAINER_URL_ARG,
        container_name_arg=CONTAINER_NAME_ARG)

    return click.Command('deploy_slc', params=opts, callback=cli_callback)


def make_args_string(**kwargs) -> str:
    def arg_string(k: str, v: Any):
        k = k.replace("_", "-")
        if isinstance(v, bool):
            return f'--{k}' if v else f'--no-{k}'
        return f'--{k} "{v}"'

    return ' '.join(arg_string(k, v) for k, v in kwargs.items())


def run_deploy_command(deploy_command: click.Command,
                       arg_string: str,
                       language_alias: str,
                       db_schema: str,
                       **db_kwargs):
    runner = CliRunner()
    runner.invoke(deploy_command, args=arg_string, catch_exceptions=False)

    with open_pyexasol_connection(**db_kwargs) as conn:
        create_schema(conn, db_schema)
        assert_udf_running(conn, language_alias, db_schema)


def test_slc_deployer_cli_onprem_url(use_onprem,
                                     container_version,
                                     language_alias,
                                     db_schema,
                                     deploy_command,
                                     onprem_cli_args,
                                     slc_cli_args):
    if not use_onprem:
        pytest.skip("The test is not configured to use ITDE.")

    extra_cli_args = {StdParams.version.name: container_version}
    arg_string = make_args_string(**onprem_cli_args, **slc_cli_args, **extra_cli_args)
    run_deploy_command(deploy_command, arg_string, language_alias, db_schema, **onprem_cli_args)


def test_slc_deployer_cli_onprem_file(use_onprem,
                                      container_path,
                                      language_alias,
                                      db_schema,
                                      deploy_command,
                                      onprem_cli_args,
                                      slc_cli_args):
    if not use_onprem:
        pytest.skip("The test is not configured to use ITDE.")

    extra_cli_args = {StdParams.container_file.name: container_path}
    arg_string = make_args_string(**onprem_cli_args, **slc_cli_args, **extra_cli_args)
    run_deploy_command(deploy_command, arg_string, language_alias, db_schema, **onprem_cli_args)
