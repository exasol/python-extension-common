from typing import Callable
from contextlib import ExitStack, contextmanager
from pathlib import Path

import pytest

from pyexasol import ExaConnection
from exasol.pytest_itde import config
import exasol.bucketfs as bfs

from exasol.python_extension_common.deployment.language_container_deployer import (
    LanguageContainerDeployer,
    LanguageActivationLevel,
)

from test.utils.revert_language_settings import revert_language_settings
from test.utils.db_utils import (create_schema, assert_udf_running)

TEST_SCHEMA = "PEC_DEPLOYER_TESTS"
TEST_LANGUAGE_ALIAS = "PYTHON3_PEC_TESTS"


def create_container_deployer(language_alias: str,
                              pyexasol_connection: ExaConnection,
                              bucketfs_config: config.BucketFs) -> LanguageContainerDeployer:

    bucketfs_path = bfs.path.build_path(backend=bfs.path.StorageBackend.onprem,
                                        url=bucketfs_config.url,
                                        username=bucketfs_config.username,
                                        password=bucketfs_config.password,
                                        service_name="bfsdefault",
                                        bucket_name="default",
                                        verify=False,
                                        path="container")
    return LanguageContainerDeployer(
        pyexasol_connection, language_alias, bucketfs_path)


def test_container_file(
        itde: config.TestConfig,
        connection_factory: Callable[[config.Exasol], ExaConnection],
        container_path: str):
    """
    Tests the deployment of a container in one call, including the activation at the System level.
    """
    with ExitStack() as stack:
        pyexasol_connection = stack.enter_context(connection_factory(itde.db))
        stack.enter_context(revert_language_settings(pyexasol_connection))
        create_schema(pyexasol_connection, TEST_SCHEMA)
        deployer = create_container_deployer(language_alias=TEST_LANGUAGE_ALIAS,
                                             pyexasol_connection=pyexasol_connection,
                                             bucketfs_config=itde.bucketfs)
        deployer.run(container_file=Path(container_path), alter_system=True, allow_override=True)
        assert_udf_running(pyexasol_connection, TEST_LANGUAGE_ALIAS, TEST_SCHEMA)

import ssl
from pyexasol import ExaConnectionFailedError

@contextmanager
# def ssl_connection(itde: config.TestConfig):
def pyexasol_ssl_connection(config: config.Exasol):
    # connection_factory(itde.db))
    # config = itde.db
    connection_params = {
        "dsn": f"{config.host}:{config.port}",
        "user": config.username,
        "password": config.password,
    )
    websocket_sslopt = { "cert_reqs": ssl.CERT_REQUIRED }
    # get_websocket_sslopt(use_ssl_cert_validation=True)
    pyexasol_conn = pyexasol.connect(
        **connection_params,
        encryption=True,
        websocket_sslopt=websocket_sslopt,
    )
    yield pyexasol_conn
    pyexasol_conn.close()


def test_cert_failure(
        itde: config.TestConfig,
        ssl_connection,
        connection_factory: Callable[[config.Exasol], ExaConnection],
        container_path: str):
    """
    Verifies that connecting with an invalid SSL certificate fails with an
    exception.
    """
    with ExitStack() as stack:
        pyexasol_connection = stack.enter_context(connection_factory(itde.db))
        stack.enter_context(revert_language_settings(pyexasol_connection))
        create_schema(pyexasol_connection, TEST_SCHEMA)
        ssl_connection = stack.enter_context(pyexasol_ssl_connection(itde.db))
        deployer = create_container_deployer(language_alias=TEST_LANGUAGE_ALIAS,
                                             pyexasol_connection=ssl_connection,
                                             bucketfs_config=itde.bucketfs)
        with pytest.raises(ExaConnectionFailedError, match="[SSL: CERTIFICATE_VERIFY_FAILED]"):
            deployer.run(container_file=Path(container_path), alter_system=True, allow_override=True)


def test_download_and_alter_session(
        itde: config.TestConfig,
        connection_factory: Callable[[config.Exasol], ExaConnection],
        container_url: str,
        container_name: str):
    """
    Tests the deployment of a container in two stages - uploading the
    container followed by activation at the Session level. This test also
    covers downloading a container file from a URL.
    """
    with ExitStack() as stack:
        pyexasol_connection = stack.enter_context(connection_factory(itde.db))
        stack.enter_context(revert_language_settings(pyexasol_connection))
        create_schema(pyexasol_connection, TEST_SCHEMA)
        deployer = create_container_deployer(language_alias=TEST_LANGUAGE_ALIAS,
                                             pyexasol_connection=pyexasol_connection,
                                             bucketfs_config=itde.bucketfs)
        deployer.download_and_run(container_url, container_name, alter_system=False)
        new_connection = stack.enter_context(connection_factory(itde.db))
        deployer = create_container_deployer(language_alias=TEST_LANGUAGE_ALIAS,
                                             pyexasol_connection=new_connection,
                                             bucketfs_config=itde.bucketfs)
        deployer.activate_container(container_name, LanguageActivationLevel.Session, True)
        assert_udf_running(new_connection, TEST_LANGUAGE_ALIAS, TEST_SCHEMA)


def test_disallow_override_makes_duplicate_alias_fail(
        itde: config.TestConfig,
        connection_factory: Callable[[config.Exasol], ExaConnection],
        container_path: str,
        container_name: str):
    """
    Tests that an attempt to activate a container fails with an exception
    when disallowing override and using an alias that already exists.
    """
    with ExitStack() as stack:
        pyexasol_connection = stack.enter_context(connection_factory(itde.db))
        stack.enter_context(revert_language_settings(pyexasol_connection))
        create_schema(pyexasol_connection, TEST_SCHEMA)
        deployer = create_container_deployer(language_alias=TEST_LANGUAGE_ALIAS,
                                             pyexasol_connection=pyexasol_connection,
                                             bucketfs_config=itde.bucketfs)
        deployer.run(container_file=Path(container_path), alter_system=True, allow_override=True)
        new_connection = stack.enter_context(connection_factory(itde.db))
        deployer = create_container_deployer(language_alias=TEST_LANGUAGE_ALIAS,
                                             pyexasol_connection=new_connection,
                                             bucketfs_config=itde.bucketfs)
        with pytest.raises(RuntimeError):
            deployer.activate_container(container_name, LanguageActivationLevel.System, False)
