import re
import exasol.bucketfs as bfs   # type: ignore
import pyexasol     # type: ignore

from datetime import datetime, timedelta
from textwrap import dedent
from typing import Callable, List
from tenacity import Retrying
from tenacity.wait import wait_fixed
from tenacity.stop import stop_after_delay

MANIFEST_FILE = "exasol-manifest.json"


def _udf_name(schema: str | None, name: str) -> str:
    timestamp = f'{datetime.now().timestamp():.0f}'
    suffix = f'"{name}_manifest_{timestamp}"'
    return f'"{schema}".{suffix}' if schema else suffix


class ExtractException(Exception):
    """
    Expected file MANIFEST_FILE could not detected on all nodes of the
    database cluster.
    """


class ExtractValidator:
    """
    This validates that a given archive (e.g. tgz) has been extracted on
    all nodes of an Exasol database cluster by checking if MANIFEST_FILE
    exists.

    The specified timeout applies to the max. total duration of both phases:
    P1) creating the UDF script and P2) checking if the UDF in SLC can be
    executed and finds extracted MANIFEST_FILE on each node.

    If a callback is specified then this function will be called multiple
    times during detecting the MANIFEST_FILE on the nodes.

    The callback is called with two arguments: the total number of nodes in
    the database cluster as returned by nproc() and a list of the IDs of the
    pending nodes on which the MANIFEST_FILE could not be found, yet.
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

    def _create_manifest_udf_with_retry(self, language_alias: str, udf_name: str):
        for attempt in Retrying(
                wait=wait_fixed(self._interval),
                stop=stop_after_delay(self._timeout),
                reraise=True):
            with attempt:
                self._create_manifest_udf(language_alias, udf_name)

    def _create_manifest_udf(self, language_alias: str, udf_name: str):
        """
        The SQL statements "ALTER SESSION SET SCRIPT_LANGUAGES" and "ALTER
        SYSTEM SET SCRIPT_LANGUAGES" doe not check whether the specified
        BucketFS path exists and has permissions allowing it to be accessed by
        UDFs.

        Much more a later statement "CREATE SCRIPT" will fail with an error
        message. Hence we need to use a retry here, as well.
        """
        self._pyexasol_conn.execute(dedent(f"""
            CREATE OR REPLACE {language_alias} SET SCRIPT
                {udf_name}(my_path VARCHAR(256))
                EMITS (node INTEGER, manifest BOOL) AS
            import os
            def run(ctx):
                ctx.emit(exa.meta.node_id, os.path.isfile(ctx.my_path))
            /
            """
        ))

    def _check_all_nodes_with_retry(self, udf_name: str, nproc: int, manifest: str, timeout: timedelta):
        for attempt in Retrying(
                wait=wait_fixed(self._interval),
                stop=stop_after_delay(timeout),
                reraise=True):
            with attempt:
                self._check_all_nodes(udf_name, nproc, manifest)

    def _check_all_nodes(self, udf_name: str, nproc: int, manifest: str):
        result = self._pyexasol_conn.execute(
            f"""
            SELECT {udf_name}('{manifest}')
            FROM VALUES BETWEEN 1 AND {nproc} t(i) GROUP BY i
            """
        ).fetchall()
        pending = list( x[0] for x in result if not x[1] )
        self._callback(nproc, pending)
        if len(pending) > 0:
            raise ExtractException(
                f"{len(pending)} of {nproc} nodes are still pending."
                f" IDs: {pending}")

    def verify_all_nodes(self, schema: str, language_alias: str, bfs_archive_path: bfs.path.PathLike):
        """
        Verify if the given bfs_archive_path was extracted on all nodes
        successfully.

        Raise an ExtractException if after the configured timeout there are
        still nodes pending, for which the extraction could not be verified,
        yet.
        """
        # manifest = f"{bfs_archive_path.as_udf_path()}/{MANIFEST_FILE}"
        manifest = "/exaudf/exaudfclient_py3"
        nproc = self._pyexasol_conn.execute("SELECT nproc()").fetchone()[0]
        udf_name = _udf_name(schema, language_alias)
        start = datetime.now()
        try:
            self._create_manifest_udf_with_retry(language_alias, udf_name)
            elapsed = datetime.now() - start
            remaining = self._timeout - elapsed
            self._check_all_nodes_with_retry(udf_name, nproc, manifest, remaining)
        finally:
            self._pyexasol_conn.execute(f"DROP SCRIPT IF EXISTS {udf_name}")
