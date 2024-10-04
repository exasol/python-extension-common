from typing import Any
from urllib.parse import urlparse
from unittest.mock import patch
from contextlib import contextmanager

import pyexasol
import pytest
import exasol.bucketfs as bfs

from exasol.python_extension_common.cli.std_options import StdParams
from exasol.python_extension_common.connections.bucketfs_location import (
    create_bucketfs_location,
    create_bucketfs_conn_object,
    create_bucketfs_location_from_conn_object,
    ConnObjectData)

TEST_FILE_CONTENT = b'A rose by any other name would smell as sweet.'


@pytest.fixture(scope='session')
def onprem_params(backend_aware_onprem_database,
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
def saas_params_with_id(saas_host,
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
def saas_params_with_name(saas_params_with_id,
                          database_name) -> dict[str, Any]:
    saas_params = dict(saas_params_with_id)
    saas_params.pop(StdParams.saas_database_id.name)
    saas_params[StdParams.saas_database_name.name] = database_name
    return saas_params


@pytest.fixture
def rubbish_params() -> dict[str, Any]:
    return {
        StdParams.bucketfs_name.name: 'bfsdefault',
        StdParams.bucket.name: 'default',
        StdParams.saas_url.name: 'my_saas_url',
    }


@contextmanager
def write_test_file(bfs_path: bfs.path.PathLike) -> None:
    bfs_path.write(TEST_FILE_CONTENT)
    try:
        yield
    finally:
        # We cannot reuse the same path, in subsequent tests because of the
        # temporary lock bucket-fs places on deleted files, but it's useful
        # to delete the file anyway to avoid potential false-positives.
        bfs_path.rm()


def validate_test_file(bfs_path: bfs.path.PathLike) -> None:
    file_content = b''.join(bfs_path.read())
    assert file_content == TEST_FILE_CONTENT


def validate_conn_object(pyexasol_connection: pyexasol.ExaConnection,
                         conn_name: str,
                         conn_obj: ConnObjectData):
    bfs_path = create_bucketfs_location_from_conn_object(conn_obj)
    validate_test_file(bfs_path)


def test_create_bucketfs_location_onprem(use_onprem,
                                         onprem_params):
    if not use_onprem:
        pytest.skip("The test is not configured to use ITDE.")

    extra_params = {StdParams.path_in_bucket.name: 'test_create_location'}
    bfs_path = create_bucketfs_location(**onprem_params, **extra_params)
    with write_test_file(bfs_path):
        validate_test_file(bfs_path)


def test_create_bucketfs_location_saas_db_id(use_saas,
                                             saas_params_with_id):
    if not use_saas:
        pytest.skip("The test is not configured to use SaaS.")

    extra_params = {StdParams.path_in_bucket.name: 'test_create_location_with_id'}
    bfs_path = create_bucketfs_location(**saas_params_with_id, **extra_params)
    with write_test_file(bfs_path):
        validate_test_file(bfs_path)


def test_create_bucketfs_location_saas_db_name(use_saas,
                                               saas_params_with_name):
    if not use_saas:
        pytest.skip("The test is not configured to use SaaS.")

    extra_params = {StdParams.path_in_bucket.name: 'test_create_location_with_name'}
    bfs_path = create_bucketfs_location(**saas_params_with_name, **extra_params)
    with write_test_file(bfs_path):
        validate_test_file(bfs_path)


def test_create_bucketfs_location_error(rubbish_params):
    with pytest.raises(ValueError):
        create_bucketfs_location(**rubbish_params)


@patch('exasol.python_extension_common.connections.bucketfs_location.write_bucketfs_conn_object')
def test_create_bucketfs_conn_object_onprem(write_conn_object_mock,
                                            use_onprem,
                                            onprem_params):
    if not use_onprem:
        pytest.skip("The test is not configured to use ITDE.")

    write_conn_object_mock.side_effect = validate_conn_object
    extra_params = {StdParams.path_in_bucket.name: 'test_create_conn_object'}
    bfs_path = create_bucketfs_location(**onprem_params, **extra_params)
    with write_test_file(bfs_path):
        create_bucketfs_conn_object(conn_name='ONPREM_TEST_BFS',
                                    **onprem_params, **extra_params)


@patch('exasol.python_extension_common.connections.bucketfs_location.write_bucketfs_conn_object')
def test_create_bucketfs_conn_object_saas_db_id(write_conn_object_mock,
                                                use_saas,
                                                saas_params_with_id):
    if not use_saas:
        pytest.skip("The test is not configured to use SaaS.")

    write_conn_object_mock.side_effect = validate_conn_object
    extra_params = {StdParams.path_in_bucket.name: 'test_create_conn_object_with_id'}
    bfs_path = create_bucketfs_location(**saas_params_with_id, **extra_params)
    with write_test_file(bfs_path):
        create_bucketfs_conn_object(conn_name='SAAS_TEST_BFS_WITH_ID',
                                    **saas_params_with_id, **extra_params)


@patch('exasol.python_extension_common.connections.bucketfs_location.write_bucketfs_conn_object')
def test_create_bucketfs_conn_object_saas_db_name(write_conn_object_mock,
                                                  use_saas,
                                                  saas_params_with_name):
    if not use_saas:
        pytest.skip("The test is not configured to use SaaS.")

    write_conn_object_mock.side_effect = validate_conn_object
    extra_params = {StdParams.path_in_bucket.name: 'test_create_conn_object_with_name'}
    bfs_path = create_bucketfs_location(**saas_params_with_name, **extra_params)
    with write_test_file(bfs_path):
        create_bucketfs_conn_object(conn_name='SAAS_TEST_BFS_WITH_NAME',
                                    **saas_params_with_name, **extra_params)


def test_create_bucketfs_conn_object_error(rubbish_params):
    with pytest.raises(ValueError):
        create_bucketfs_conn_object(conn_name='WHATEVER', **rubbish_params)
