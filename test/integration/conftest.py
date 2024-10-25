from __future__ import annotations
from typing import Any
import pytest
import click
import requests
from urllib.parse import urlparse
from contextlib import ExitStack, contextmanager
import pyexasol
import exasol.bucketfs as bfs

from exasol.python_extension_common.cli.std_options import StdParams
from exasol.python_extension_common.deployment.language_container_deployer import (
    LanguageContainerDeployer,
)
from exasol.python_extension_common.deployment.language_container_deployer_cli import (
    language_container_deployer_main, slc_parameter_formatters, CustomizableParameters)
from test.utils.revert_language_settings import revert_language_settings
from test.utils.db_utils import create_schema, open_schema

VERSION = "8.3.0"

TEST_SCHEMA = "PEC_DEPLOYER_TESTS"
TEST_LANGUAGE_ALIAS = "PYTHON3_PEC_TESTS"


@pytest.fixture(scope='session')
def container_name() -> str:
    return "template-Exasol-all-python-3.10_release.tar.gz"


@pytest.fixture(scope='session')
def container_url_formatter(container_name) -> str:
    return ("https://github.com/exasol/script-languages-release/releases/"
            "download/{version}/") + container_name


@pytest.fixture
def main_func(slc_name, slc_url_formatter):

    @click.group()
    def fake_main():
        pass

    slc_parameter_formatters.set_formatter(CustomizableParameters.container_url, container_url_formatter)
    slc_parameter_formatters.set_formatter(CustomizableParameters.container_name, container_name)

    fake_main.add_command(language_container_deployer_main)
    return fake_main


@pytest.fixture(scope='session')
def container_version() -> str:
    return VERSION


@pytest.fixture(scope='session')
def container_url(container_url_formatter, container_version) -> str:
    return container_url_formatter.format(version=container_version)


@pytest.fixture(scope='session')
def container_path(tmpdir_factory, container_url, container_name) -> str:

    response = requests.get(container_url, allow_redirects=True)
    response.raise_for_status()
    slc_path = tmpdir_factory.mktemp('container').join(container_name)
    slc_path = str(slc_path)
    with open(slc_path, 'wb') as f:
        f.write(response.content)
    return slc_path


@pytest.fixture(scope='session')
def db_schema() -> str:
    return TEST_SCHEMA


@pytest.fixture(scope='session')
def language_alias() -> str:
    return TEST_LANGUAGE_ALIAS


@pytest.fixture(scope='session')
def deployer_factory(
        backend_aware_database_params,
        backend_aware_bucketfs_params,
        db_schema,
        language_alias):
    @contextmanager
    def create_deployer(create_test_schema: bool = False, open_test_schema: bool = True):
        with ExitStack() as stack:
            pyexasol_connection = stack.enter_context(pyexasol.connect(**backend_aware_database_params))
            bucketfs_path = bfs.path.build_path(**backend_aware_bucketfs_params)
            stack.enter_context(revert_language_settings(pyexasol_connection))
            if create_test_schema:
                create_schema(pyexasol_connection, db_schema, open_test_schema)
            elif open_test_schema:
                open_schema(pyexasol_connection, db_schema)
            yield LanguageContainerDeployer(pyexasol_connection, language_alias, bucketfs_path)
    return create_deployer


@pytest.fixture(scope='session')
def onprem_db_params(backend_aware_onprem_database,
                     exasol_config) -> dict[str, Any]:
    return {
        StdParams.dsn.name: f'{exasol_config.host}:{exasol_config.port}',
        StdParams.db_user.name: exasol_config.username,
        StdParams.db_password.name: exasol_config.password,
        StdParams.use_ssl_cert_validation.name: False
    }


@pytest.fixture(scope='session')
def onprem_bfs_params(backend_aware_onprem_database,
                      bucketfs_config) -> dict[str, Any]:
    parsed_url = urlparse(bucketfs_config.url)
    host, port = parsed_url.netloc.split(":")
    return {
        StdParams.bucketfs_host.name: host,
        StdParams.bucketfs_port.name: port,
        StdParams.bucketfs_use_https.name: parsed_url.scheme.lower() == 'https',
        StdParams.bucketfs_user.name: bucketfs_config.username,
        StdParams.bucketfs_password.name: bucketfs_config.password,
        StdParams.bucketfs_name.name: 'bfsdefault',
        StdParams.bucket.name: 'default',
        StdParams.use_ssl_cert_validation.name: False
    }


@pytest.fixture(scope='session')
def saas_params_id(saas_host,
                   saas_pat,
                   saas_account_id,
                   backend_aware_saas_database_id) -> dict[str, Any]:
    return {
        StdParams.saas_url.name: saas_host,
        StdParams.saas_account_id.name: saas_account_id,
        StdParams.saas_database_id.name: backend_aware_saas_database_id,
        StdParams.saas_token.name: saas_pat,
    }


@pytest.fixture(scope='session')
def saas_params_name(saas_params_id,
                     database_name) -> dict[str, Any]:
    saas_params = dict(saas_params_id)
    saas_params.pop(StdParams.saas_database_id.name)
    saas_params[StdParams.saas_database_name.name] = database_name
    return saas_params
