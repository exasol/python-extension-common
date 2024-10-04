from exasol.python_extension_common.connections.bucketfs_location import (
    create_bucketfs_conn_object)


class BucketfsConnObjectCli:
    def __init__(self, conn_name_arg: str):
        self._conn_name_arg = conn_name_arg

    def __call__(self, **kwargs):
        create_bucketfs_conn_object(conn_name=self._conn_name_arg, **kwargs)
