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


def _udf_name(schema: str | None, name: str = "manifest") -> str:
    return f'"{schema}"."{name}"' if schema else f'"{name}"'


class ExtractException(Exception):
    """
    Expected file MANIFEST_FILE could not detected on all nodes of the
    database cluster.
    """


def manifest_path(bfs_path: bfs.path.PathLike) -> str | None:
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
                 timeout: timedelta,
                 interval: timedelta = timedelta(seconds=10),
                 callback: Callable[[int, List[int]], None] | None = None,
                 ) -> None:
        self._pyexasol_conn = pyexasol_connection
        self._timeout = timeout
        self._interval = interval
        self._callback = callback if callback else lambda x, y: None

    def _delete_manifest_udf(self, language_alias: str, schema: str):
        self._pyexasol_conn.execute(f"DROP SCRIPT IF EXISTS {_udf_name(schema)}")

    def _create_manifest_udf(self, language_alias: str, schema: str):
        # how to handle potential errors?
        self._pyexasol_conn.execute(
            f"""
            CREATE OR REPLACE {language_alias} SCALAR SCRIPT
            {_udf_name(schema)}(my_path VARCHAR(256)) RETURNS BOOL AS
            import os
            def run(ctx):
                return os.path.isfile(ctx.my_path)
            /
            """
        )

    def verify_all_nodes(self, schema: str, language_alias: str, bucketfs_path: bfs.path.PathLike):
        """
        Verify if the given bucketfs_path was extracted on all nodes
        successfully.

        Raise an ExtractException if the specified bucketfs_path was not an
        archive or if after the configured timeout there are still nodes
        pending, for which the extraction could not be verified, yet.
        """
        @retry(wait=wait_fixed(self._interval), stop=stop_after_delay(self._timeout), reraise=True)
        def check_all_nodes(nproc, manifest):
            result = self._pyexasol_conn.execute(
                f"""
                SELECT iproc() "Node", {_udf_name(schema)}('{manifest}') "Manifest"
                FROM VALUES BETWEEN 1 AND {nproc} GROUP BY iproc()
                """
            )
            pending = list( x[0] for x in result if not x[1] )
            self._callback(nproc, pending)
            if len(pending) > 0:
                raise ExtractException(
                    f"{len(pending)} of {nproc} nodes are still pending."
                    f" IDs: {pending}")

        manifest = manifest_path(bucketfs_path)
        if manifest is None:
            raise ExtractException(
                f"{bucketfs_path} does not point to an archive"
                f" which could contain a file {MANIFEST_FILE}")
        nproc = self._pyexasol_conn.execute("SELECT nproc()")
        try:
            self._create_manifest_udf(language_alias, schema)
            check_all_nodes(nproc, manifest)
        finally:
            self._delete_manifest_udf(language_alias, schema)
