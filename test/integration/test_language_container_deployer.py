from pathlib import Path
from urllib.parse import urlparse

import pyexasol
from exasol.pytest_backend import BACKEND_ONPREM
import pytest

from exasol.python_extension_common.deployment.language_container_deployer import (
    LanguageContainerDeployer,
    LanguageActivationLevel,
)
from test.utils.db_utils import assert_udf_running


def test_container_file(deployer_factory,
                        db_schema,
                        language_alias,
                        container_path):
    """
    Tests the deployment of a container in one call, including the activation at the System level.
    """
    with deployer_factory(create_test_schema=True) as deployer:
        deployer.run(container_file=Path(container_path), alter_system=True, allow_override=True)
        assert_udf_running(deployer.pyexasol_connection, language_alias, db_schema)


def test_cert_failure(backend,
                      exasol_config,
                      bucketfs_config,
                      language_alias):
    """
    Verifies that connecting with an invalid SSL certificate fails with an
    exception.

    Arguably, this is a redundant test. In effect, we are testing that it is not possible
    to open a pyexasol connection requesting an SSL certificate validation when the server
    cannot provide one. This test case is not directly related to the language container
    deployer.
    """
    if backend != BACKEND_ONPREM:
        pytest.skip(("We run this test only with the Docker-DB "
                     "because SaaS always verifies the SSL certificate"))
    parsed_url = urlparse(bucketfs_config.url)
    with pytest.raises(pyexasol.ExaConnectionFailedError, match="[SSL: CERTIFICATE_VERIFY_FAILED]"):
        deployer = LanguageContainerDeployer.create(
            language_alias=language_alias,
            dsn=f"{exasol_config.host}:{exasol_config.port}",
            db_user=exasol_config.username,
            db_password=exasol_config.password,
            bucketfs_name="bfsdefault",
            bucketfs_host=parsed_url.hostname,
            bucketfs_port=parsed_url.port,
            bucketfs_use_https=False,
            bucketfs_user=bucketfs_config.username,
            bucketfs_password=bucketfs_config.password,
            bucket="default",
            use_ssl_cert_validation=True,
            path_in_bucket="container",
            )


def test_download_and_alter_session(
        deployer_factory,
        db_schema,
        language_alias,
        container_url,
        container_name):
    """
    Tests the deployment of a container in 3 stages - 1. download a
    container file from a URL, 2. upload the file to the BucketFS and
    3. activate it at the Session level.
    """
    with deployer_factory(create_test_schema=True) as deployer1:
        deployer1.download_and_run(container_url, container_name, alter_system=False)
        with deployer_factory(create_test_schema=False) as deployer2:
            deployer2.activate_container(container_name, LanguageActivationLevel.Session, True)
            assert_udf_running(deployer2.pyexasol_connection, language_alias, db_schema)


def test_disallow_override_makes_duplicate_alias_fail(
        deployer_factory,
        container_path: str,
        container_name: str):
    """
    Tests that an attempt to activate a container fails with an exception
    when disallowing override and using an alias that already exists.
    """
    with deployer_factory(create_test_schema=True) as deployer1:
        deployer1.run(container_file=Path(container_path), alter_system=True, allow_override=True)
        with deployer_factory(create_test_schema=False) as deployer2:
            with pytest.raises(RuntimeError):
                deployer2.activate_container(
                    bucket_file_path=container_name,
                    alter_type=LanguageActivationLevel.System,
                    allow_override=False,
                )
