from unittest.mock import (
    call,
    patch,
)

from exasol.python_extension_common.cli.bucketfs_conn_object_cli import (
    BucketfsConnObjectCli,
)


@patch("exasol.python_extension_common.connections.bucketfs_location.create_bucketfs_conn_object")
def test_bucketfs_conn_object_cli(create_con_object_mock):
    conn_name = "my_conn_name"
    fake_params = {"x": "xxx", "y": "yyy"}
    conn_object_callback = BucketfsConnObjectCli("conn_name")
    conn_object_callback(conn_name=conn_name, **fake_params)
    assert create_con_object_mock.call_args_list == [call(conn_name=conn_name, **fake_params)]
