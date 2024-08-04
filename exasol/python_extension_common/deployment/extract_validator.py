import re
import exasol.bucketfs as bfs   # type: ignore
import pyexasol     # type: ignore

from datetime import timedelta
from typing import Callable, List
from tenacity import retry
from tenacity.wait import wait_fixed
from tenacity.stop import stop_after_delay

from exasol.python_extension_common.deployment.language_container_validator import (
    temp_schema
)

MANIFEST_FILE = "exasol-manifest.json"

class ExtractException(Exception):
    """
    Expected file MANIFEST_FILE could not detected on all nodes of the
    database cluster.
    """


def manifest_path(bfs_path: bfs.path.PathLike) -> str:
    parent = bfs.path.BucketPath(bfs_path._path.parent, bfs_path._bucket_api)
    regex = re.compile(r"(.*)\.(tar|tgz|tar\.gz|zip|gzip)$")
    match = regex.match(bfs_path.name)
    if not match:
        return None
    manifest = parent / match.group(1) / MANIFEST_FILE
    return manifest.as_udf_path()


class ExtractValidator:
    """
    This validates that a given archive (e.g. tgz) has been extracted on
    all nodes of an Exasol database cluster by checking if MANIFEST_FILE
    exists.
    """
    def __init__(self,
                 pyexasol_connection: pyexasol.ExaConnection,
                 bucketfs_path: bfs.path.PathLike,
                 timeout: timedelta,
                 interval: timedelta = timedelta(seconds=10),
                 callback: Callable[[int, List[int]], None]= None,
                 ) -> None:
        self._pyexasol_conn = pyexasol_connection
        self._bucketfs_path = bucketfs_path
        self._timeout = timeout
        self._interval = interval
        self._callback = callback if callback else lambda x: None

    def _create_manifest_udf(self, schema: str):
        # how to handle potential errors?
        self._pyexasol_conn.execute(
            f"""
            CREATE OR REPLACE PYTHON3 SCALAR SCRIPT
            "{schema}".manifest(my_path VARCHAR(256)) RETURNS BOOL AS
            import os
            def run(ctx):
                return os.path.isfile(ctx.my_path)
            /
            """
        )

    def is_extracted_on_all_nodes(self) -> bool:
        """
        Return list of the IDs of the pending cluster nodes.

        A node is "pending" if the successful extraction of the manifest could
        not be detected, yet.
        """
        @retry(wait=wait_fixed(self._interval), stop=stop_after_delay(self._timeout), reraise=True)
        def check_all_nodes(total_nodes, manifest) -> List[int]:
            result = self._pyexasol_conn.execute(
                f"""
                select iproc() "Node", manifest('{manifest}') "Manifest"
                from values between 0 and {total_nodes - 1} group by iproc()
                """

            )
            pending = list( x[0] for x in result if not x[1] )
            self._callback(total_nodes, pending)
            if len(pending) > 0:
                raise ExtractException(
                    f"{len(pending)} of {total_nodes} nodes are still pending."
                    f" IDs: {pending}"
                )

        manifest = manifest_path(self._bucketfs_path)
        if manifest is None:
            return False
        total_nodes = self._pyexasol_conn.execute("select nproc()")
        with temp_schema(self._pyexasol_conn) as schema:
            self._create_manifest_udf(schema)
            check_all_nodes(total_nodes, manifest)
            return True
