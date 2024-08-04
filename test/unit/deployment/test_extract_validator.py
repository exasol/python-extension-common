import logging
import pytest
import re
import exasol.bucketfs as bfs   # type: ignore

from typing import Any, Dict, List
from unittest.mock import Mock, call
from datetime import timedelta
from pathlib import Path

from exasol.python_extension_common.deployment.extract_validator import (
    ExtractValidator,
    ExtractException,
    manifest_path,
)
from tenacity import RetryError

LOG = logging.getLogger(__name__)


def bucket_path(path: str):
    bucket_api = bfs.MountedBucket("svc", "bkt")
    return bfs.path.BucketPath(path, bucket_api=bucket_api)


@pytest.mark.parametrize(
    "bfs_path", [
        "folder/archive.tar.gz",
        "folder/archive.tgz",
        "folder/archive.zip",
        "folder/archive.tar",
        "folder/archive.gzip",
    ] )
def test_manifest_path(bfs_path):
    path = bucket_path(bfs_path)
    # print(f'{manifest_path(path)}')
    assert "/buckets/svc/bkt/folder/archive/exasol-manifest.json" == manifest_path(path)


@pytest.mark.parametrize(
    "bfs_path", [
        "folder/folder/",
        "folder/file.txt",
    ] )
def test_manifest_path_none(bfs_path):
    path = bucket_path(bfs_path)
    assert manifest_path(path) is None


class ConnectionMock:
    def __init__(self, spec: Dict[str, Any]):
        self.spec = spec

    def execute(self, *args, **kwargs):
        statement = args[0] if len(args) else kwargs["query"]
        first_line = statement.strip().splitlines()[0]
        for regex, value in self.spec.items():
            if re.match(regex, first_line, re.IGNORECASE):
                return value() if callable(value) else value
        LOG.warning(f"ConnectionMock.execute() called with '{first_line[:40]}...'")
        return None


class Simulator:
    def __init__(self, nodes: int, udf_results: List[List[any]]):
        self.nodes = nodes
        self.udf = Mock(side_effect=udf_results)
        self.callback = Mock()

    def _callback(self, n, pending):
        LOG.debug(f"{len(pending)} of {n} nodes pending: {pending}")

    @property
    def testee(self):
        connection = ConnectionMock({
            r"(CREATE|DROP) ": None,
            r"SELECT nproc\(\)": self.nodes,
            r"SELECT .* manifest\(": self.udf,
        })
        pyexasol = Mock()
        pyexasol.execute = connection.execute
        self.callback.side_effect = self._callback
        return ExtractValidator(
            pyexasol,
            bucket_path("/folder/a.tgz"),
            timeout=timedelta(milliseconds=20),
            interval=timedelta(milliseconds=10),
            callback=self.callback,
        )


def test_no_archive():
    pyexasol = Mock(),
    testee = ExtractValidator(
        pyexasol,
        bucket_path("/folder/a.txt"),
        timeout=timedelta(milliseconds=20),
    )
    assert not testee.is_deployed()


def test_failure():
    sim = Simulator(
        nodes=4,
        udf_results=[
            [[1, False]],
            [[1, False]],
            [[1, False]],
        ])
    with pytest.raises(ExtractException) as ex:
        assert sim.testee.is_deployed()
    assert "1 of 4 nodes are still pending. IDs: [1]" == str(ex.value)


def test_success():
    sim = Simulator(
        nodes=4,
        udf_results=[
            [[1, False], [2, False]],
            [[1, True], [2, False]],
            [[1, True], [2, True]],
        ])
    try:
        assert sim.testee.is_deployed()
    except ExtractException as ex:
        print(f'{ex}')
    assert sim.callback.call_args_list == [
        call(4, [1,2]),
        call(4, [2]),
        call(4, []),
    ]
