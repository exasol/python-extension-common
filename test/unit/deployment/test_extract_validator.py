import logging
import pytest
import re
import time
import exasol.bucketfs as bfs   # type: ignore
from pyexasol import ExaConnection

from typing import Any, Dict, List
from unittest.mock import Mock, call
from datetime import timedelta
from pathlib import Path

from exasol.python_extension_common.deployment.extract_validator import (
    ExtractValidator,
    ExtractException,
    _udf_name,
)
from tenacity import RetryError

LOG = logging.getLogger(__name__)


def bucket_path(path: str):
    bucket_api = bfs.MountedBucket("svc", "bkt")
    return bfs.path.BucketPath(path, bucket_api=bucket_api)


@pytest.fixture
def archive_bucket_path():
    return bucket_path("/folder/a.tgz")


class ConnectionMock:
    def __init__(self, spec: Dict[str, Any]):
        self.spec = spec
        self.values = iter(())

    def _get_values(self, first_line: str):
        for regex, values in self.spec.items():
            if re.match(regex, first_line, re.IGNORECASE):
                return values() if callable(values) else values
        LOG.warning(f"ConnectionMock.execute() called with '{first_line[:40]}...'")
        return ()

    def execute(self, *args, **kwargs):
        statement = args[0] if len(args) else kwargs["query"]
        first_line = statement.strip().splitlines()[0]
        self.values = iter(self._get_values(first_line))
        return self

    def fetchone(self):
        return next(self.values)

    def fetchall(self):
        return [ v for v in self.values ]


class Simulator:
    def __init__(self, nodes: int, udf_results: List[List[any]],
                 create_script=()):
        self.create_script = create_script
        self.nodes = nodes
        self.udf = Mock(side_effect=udf_results)
        self.callback = Mock(side_effect = self._callback)

    def _callback(self, n, pending):
        LOG.debug(f"{len(pending)} of {n} nodes pending: {pending}")

    @property
    def testee(self):
        connection = ConnectionMock({
            r"CREATE .* SCRIPT": self.create_script,
            r"(CREATE|DROP) ": (),
            r"SELECT nproc\(\)": [ self.nodes ],
            r'SELECT .*_manifest_': self.udf,
        })
        return ExtractValidator(
            pyexasol_connection=Mock(execute=connection.execute),
            timeout=timedelta(milliseconds=20),
            interval=timedelta(milliseconds=10),
            callback=self.callback,
        )


@pytest.mark.parametrize(
    "schema, expected",
    [
        (None, r'"alias_manifest_[0-9]+"'),
        ("schema", r'"schema"\."alias_manifest_[0-9]+"'),
    ])
def test_udf_name(schema, expected):
    assert re.match(expected, _udf_name(schema, "alias"))


def test_create_script_failure(archive_bucket_path):
    create_script = Mock(side_effect=Exception("failed to create UDF script"))
    sim = Simulator(nodes=4, udf_results=[], create_script=create_script)
    with pytest.raises(Exception, match="failed to create UDF script") as ex:
        assert sim.testee.verify_all_nodes("alias", "schema", archive_bucket_path)


def test_failure(archive_bucket_path):
    sim = Simulator(
        nodes=4,
        udf_results=[
            [[1, False]],
            [[1, False]],
            [[1, False]],
        ])
    with pytest.raises(ExtractException) as ex:
        sim.testee.verify_all_nodes("alias", "schema", archive_bucket_path)
    assert "1 of 4 nodes are still pending. IDs: [1]" == str(ex.value)


def test_success(archive_bucket_path):
    sim = Simulator(
        nodes=4,
        udf_results=[
            [[1, False], [2, False]],
            [[1, True ], [2, False]],
            [[1, True ], [2, True ]],
        ])
    sim.testee.verify_all_nodes("alias", "schema", archive_bucket_path)
    assert sim.callback.call_args_list == [
        call(4, [1, 2]),
        call(4, [2]),
        call(4, []),
    ]


def test_reduced_timeout(archive_bucket_path):
    """
    This test simulates a retry being required for creating the UDF
    script, hence already eating up part of the total timeout.

    The test then verifies the remaining part of the total timeout for actual
    calls to the UDF being too short for successfully detecting the manifest
    on all nodes.
    """
    create_script = Mock(side_effect=[Exception("failure"), ()])
    udf_results=[
        [[1, False], [2, False]],
        [[1, True ], [2, False]],
        [[1, True ], [2, True ]],
    ]
    sim = Simulator(
        nodes=4,
        udf_results=udf_results,
        create_script=create_script,
    )
    with pytest.raises(ExtractException) as ex:
        sim.testee.verify_all_nodes("alias", "schema", archive_bucket_path)
    assert "1 of 4 nodes are still pending. IDs: [2]" == str(ex.value)
