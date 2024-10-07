import exasol.python_extension_common.connections.bucketfs_location as bl


class BucketfsConnObjectCli:
    def __init__(self, conn_name_arg: str):
        self._conn_name_arg = conn_name_arg

    def __call__(self, **kwargs):
        conn_name = kwargs.pop(self._conn_name_arg)
        bl.create_bucketfs_conn_object(conn_name=conn_name, **kwargs)
