from typing import Any
from contextlib import ExitStack
from urllib.parse import urlparse
import pytest
import click
from click.testing import CliRunner

from exasol.python_extension_common.cli.std_options import (
    StdTags, StdParams, ParameterFormatters, select_std_options, kwargs_to_cli_args)
from exasol.python_extension_common.connections.pyexasol_connection import (
    open_pyexasol_connection)
from exasol.python_extension_common.cli.language_container_deployer_cli import (
    LanguageContainerDeployerCli)
from test.utils.db_utils import (assert_udf_running, create_schema)
from test.utils.revert_language_settings import revert_language_settings

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
        StdParams.saas_token.name: saas_pat
    }


@pytest.fixture(scope='session')
def slc_cli_args(language_alias) -> dict[str, Any]:
    return {
        StdParams.alter_system.name: True,
        StdParams.allow_override.name: True,
        StdParams.wait_for_completion.name: True,
        StdParams.path_in_bucket.name: 'container',
        StdParams.language_alias.name: language_alias
    }


def create_deploy_command(backend_tag: StdTags,
                          container_name: str | None = None,
                          container_url_formatter: str | None = None) -> click.Command:
    """
    This is a blueprint for creating an isolated click Command
    for the language container deployment.

    backend_tag should be either StdTags.ONPREM or StdTags.SAAS.
    """
    if container_name and container_url_formatter:
        ver_formatter = ParameterFormatters()
        ver_formatter.set_formatter(CONTAINER_URL_ARG, container_url_formatter)
        ver_formatter.set_formatter(CONTAINER_NAME_ARG, container_name)
        formatters = {StdParams.version: ver_formatter}
    else:
        formatters = None

    opts = select_std_options(
        [StdTags.DB | backend_tag, StdTags.BFS | backend_tag, StdTags.SLC],
        formatters=formatters)
    cli_callback = LanguageContainerDeployerCli(
        container_url_arg=CONTAINER_URL_ARG,
        container_name_arg=CONTAINER_NAME_ARG)

    return click.Command('deploy_slc', params=opts, callback=cli_callback)


def run_deploy_command(deploy_command: click.Command,
                       arg_string: str,
                       language_alias: str,
                       db_schema: str,
                       db_params: dict[str, Any]):

    with ExitStack() as stack:
        conn_before = stack.enter_context(open_pyexasol_connection(**db_params))
        stack.enter_context(revert_language_settings(conn_before))

        runner = CliRunner()
        runner.invoke(deploy_command, args=arg_string,
                      catch_exceptions=False, standalone_mode=False)

        # We have to open another connection because the language settings on
        # the previously opened connection are unaffected by the slc deployment.
        conn_after = stack.enter_context(open_pyexasol_connection(**db_params))
        create_schema(conn_after, db_schema)
        assert_udf_running(conn_after, language_alias, db_schema)


def test_slc_deployer_cli_onprem_url(use_onprem,
                                     container_version,
                                     container_name,
                                     container_url_formatter,
                                     language_alias,
                                     db_schema,
                                     onprem_cli_args,
                                     slc_cli_args):
    if not use_onprem:
        pytest.skip("The test is not configured to use ITDE.")

    deploy_command = create_deploy_command(StdTags.ONPREM,
                                           container_name=container_name,
                                           container_url_formatter=container_url_formatter)
    extra_cli_args = {StdParams.version.name: container_version}
    arg_string = kwargs_to_cli_args(**onprem_cli_args, **slc_cli_args, **extra_cli_args)
    run_deploy_command(deploy_command, arg_string, language_alias, db_schema, onprem_cli_args)


def test_slc_deployer_cli_onprem_file(use_onprem,
                                      container_path,
                                      language_alias,
                                      db_schema,
                                      onprem_cli_args,
                                      slc_cli_args):
    if not use_onprem:
        pytest.skip("The test is not configured to use ITDE.")

    deploy_command = create_deploy_command(StdTags.ONPREM)
    extra_cli_args = {StdParams.container_file.name: container_path}
    arg_string = kwargs_to_cli_args(**onprem_cli_args, **slc_cli_args, **extra_cli_args)
    run_deploy_command(deploy_command, arg_string, language_alias, db_schema, onprem_cli_args)


def test_slc_deployer_cli_saas_url(use_saas,
                                   container_version,
                                   container_name,
                                   container_url_formatter,
                                   language_alias,
                                   db_schema,
                                   saas_cli_args,
                                   slc_cli_args):
    if not use_saas:
        pytest.skip("The test is not configured to run in SaaS.")

    deploy_command = create_deploy_command(StdTags.SAAS,
                                           container_name=container_name,
                                           container_url_formatter=container_url_formatter)
    extra_cli_args = {StdParams.version.name: container_version}
    arg_string = kwargs_to_cli_args(**saas_cli_args, **slc_cli_args, **extra_cli_args)
    run_deploy_command(deploy_command, arg_string, language_alias, db_schema, saas_cli_args)


def test_slc_deployer_cli_saas_file(use_saas,
                                    container_path,
                                    language_alias,
                                    db_schema,
                                    saas_cli_args,
                                    slc_cli_args):
    if not use_saas:
        pytest.skip("The test is not configured to run in SaaS.")

    deploy_command = create_deploy_command(StdTags.SAAS)
    extra_cli_args = {StdParams.container_file.name: container_path}
    arg_string = kwargs_to_cli_args(**saas_cli_args, **slc_cli_args, **extra_cli_args)
    run_deploy_command(deploy_command, arg_string, language_alias, db_schema, saas_cli_args)
