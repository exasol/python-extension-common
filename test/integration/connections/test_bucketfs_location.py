from contextlib import contextmanager
from typing import Any
from unittest.mock import patch

import exasol.bucketfs as bfs
import pyexasol
import pytest

from exasol.python_extension_common.cli.std_options import StdParams
from exasol.python_extension_common.connections.bucketfs_location import (
    ConnectionInfo,
    create_bucketfs_conn_object,
    create_bucketfs_location,
    create_bucketfs_location_from_conn_object,
)

TEST_FILE_CONTENT = b"A rose by any other name would smell as sweet."


@pytest.fixture
def rubbish_params() -> dict[str, Any]:
    return {
        StdParams.bucketfs_name.name: "bfsdefault",
        StdParams.bucket.name: "default",
        StdParams.saas_url.name: "my_saas_url",
    }


@contextmanager
def write_test_file(bfs_path: bfs.path.PathLike) -> None:
    try:
        bfs_path.write(TEST_FILE_CONTENT)
        yield
    finally:
        # We cannot reuse the same path, in subsequent tests because of the
        # temporary lock bucket-fs places on deleted files, but it's useful
        # to delete the file anyway to avoid potential false-positives.
        bfs_path.rm()


def validate_test_file(bfs_path: bfs.path.PathLike) -> None:
    file_content = b"".join(bfs_path.read())
    assert file_content == TEST_FILE_CONTENT


def validate_conn_object(
    pyexasol_connection: pyexasol.ExaConnection, conn_name: str, conn_obj: ConnectionInfo
):
    bfs_path = create_bucketfs_location_from_conn_object(conn_obj)
    validate_test_file(bfs_path)


def test_create_bucketfs_location_onprem(use_onprem, onprem_bfs_params):
    if not use_onprem:
        pytest.skip("The test is not configured to use ITDE.")

    extra_params = {StdParams.path_in_bucket.name: "test_create_location"}
    bfs_path = create_bucketfs_location(**onprem_bfs_params, **extra_params)
    with write_test_file(bfs_path):
        validate_test_file(bfs_path)


def test_create_bucketfs_location_saas_db_id(use_saas, saas_params_id):
    if not use_saas:
        pytest.skip("The test is not configured to use SaaS.")

    extra_params = {StdParams.path_in_bucket.name: "test_create_location_with_id"}
    bfs_path = create_bucketfs_location(**saas_params_id, **extra_params)
    with write_test_file(bfs_path):
        validate_test_file(bfs_path)


def test_create_bucketfs_location_saas_db_name(use_saas, saas_params_name):
    if not use_saas:
        pytest.skip("The test is not configured to use SaaS.")

    extra_params = {StdParams.path_in_bucket.name: "test_create_location_with_name"}
    bfs_path = create_bucketfs_location(**saas_params_name, **extra_params)
    with write_test_file(bfs_path):
        validate_test_file(bfs_path)


def test_create_bucketfs_location_error(rubbish_params):
    with pytest.raises(ValueError):
        create_bucketfs_location(**rubbish_params)


@patch("exasol.python_extension_common.connections.bucketfs_location.write_bucketfs_conn_object")
def test_create_bucketfs_conn_object_onprem(
    write_conn_object_mock, use_onprem, onprem_db_params, onprem_bfs_params
):
    if not use_onprem:
        pytest.skip("The test is not configured to use ITDE.")

    write_conn_object_mock.side_effect = validate_conn_object
    extra_params = {StdParams.path_in_bucket.name: "test_create_conn_object"}
    bfs_path = create_bucketfs_location(**onprem_bfs_params, **extra_params)
    with write_test_file(bfs_path):
        # onprem_db_params and onprem_bfs_params have one item in common -
        # use_ssl_cert_validation, so we need to take a union before using them as kwargs.
        onprem_params = dict(onprem_db_params)
        onprem_params.update(onprem_bfs_params)
        create_bucketfs_conn_object(conn_name="ONPREM_TEST_BFS", **onprem_params, **extra_params)


@patch("exasol.python_extension_common.connections.bucketfs_location.write_bucketfs_conn_object")
def test_create_bucketfs_conn_object_saas_db_id(write_conn_object_mock, use_saas, saas_params_id):
    if not use_saas:
        pytest.skip("The test is not configured to use SaaS.")

    write_conn_object_mock.side_effect = validate_conn_object
    extra_params = {StdParams.path_in_bucket.name: "test_create_conn_object_with_id"}
    bfs_path = create_bucketfs_location(**saas_params_id, **extra_params)
    with write_test_file(bfs_path):
        create_bucketfs_conn_object(conn_name="SAAS_TEST_BFS_ID", **saas_params_id, **extra_params)


@patch("exasol.python_extension_common.connections.bucketfs_location.write_bucketfs_conn_object")
def test_create_bucketfs_conn_object_saas_db_name(
    write_conn_object_mock, use_saas, saas_params_name
):
    if not use_saas:
        pytest.skip("The test is not configured to use SaaS.")

    write_conn_object_mock.side_effect = validate_conn_object
    extra_params = {StdParams.path_in_bucket.name: "test_create_conn_object_with_name"}
    bfs_path = create_bucketfs_location(**saas_params_name, **extra_params)
    with write_test_file(bfs_path):
        create_bucketfs_conn_object(
            conn_name="SAAS_TEST_BFS_NAME", **saas_params_name, **extra_params
        )


def test_create_bucketfs_conn_object_error(rubbish_params):
    with pytest.raises(ValueError):
        create_bucketfs_conn_object(conn_name="WHATEVER", **rubbish_params)
