from __future__ import annotations
import pytest
import click
import requests
from contextlib import ExitStack, contextmanager
import pyexasol
import exasol.bucketfs as bfs

from exasol.python_extension_common.deployment.language_container_deployer import (
    LanguageContainerDeployer,
)
from exasol.python_extension_common.deployment.language_container_deployer_cli import (
    language_container_deployer_main, slc_parameter_formatters, CustomizableParameters)
from test.utils.revert_language_settings import revert_language_settings
from test.utils.db_utils import create_schema, open_schema

VERSION = "8.0.0"

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
