import contextlib
import logging
import re
from datetime import timedelta
from typing import (
    Any,
    Dict,
    List,
)
from unittest.mock import (
    Mock,
    call,
    patch,
)

import exasol.bucketfs as bfs  # type: ignore
import pytest
from pyexasol import ExaConnection
from tenacity import RetryError

from exasol.python_extension_common.deployment.extract_validator import (
    ExtractException,
    ExtractValidator,
    _udf_name,
)

LOG = logging.getLogger(__name__)


def bucket_path(path: str):
    bucket_api = bfs.MountedBucket("svc", "bkt")
    return bfs.path.BucketPath(path, bucket_api=bucket_api)


@pytest.fixture
def archive_bucket_path():
    return bucket_path("/folder/a.tgz")


class ConnectionMock:
    def __init__(self, spec: dict[str, Any]):
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
        return [next(self.values)]

    def fetchall(self):
        return [v for v in self.values]


class Simulator:
    def __init__(self, nodes: int, udf_results: list[list[any]], create_script=()):
        self.create_script = create_script
        self.nodes = nodes
        self.udf = Mock(side_effect=udf_results)
        self.callback = Mock(side_effect=self._callback)

    def _callback(self, n, pending):
        LOG.debug(f"{len(pending)} of {n} nodes pending: {pending}")

    @property
    def testee(self):
        connection = ConnectionMock(
            {
                r"CREATE .* SCRIPT": self.create_script,
                r"(CREATE|DROP) ": (),
                r"SELECT nproc\(\)": [self.nodes],
                r"SELECT .*_manifest_": self.udf,
            }
        )
        return ExtractValidator(
            pyexasol_connection=Mock(execute=connection.execute),
            timeout=timedelta(seconds=10),
            interval=timedelta(seconds=1),
            callback=self.callback,
        )


@contextlib.contextmanager
def mock_tenacity_wait(*wait_lists: list[int | float], max: int = 1000):
    """
    This context mocks internals of library ``tenacity`` in order to
    simulate waiting for timeouts in ``tenacity.Retrying()``. All specified
    durations are interpreted as number of seconds which can be floats.

    A test case may provide multiple lists of waiting periods to cover
    multiple consecutive retry phases in the class under test, see
    ``ExtractValidator`` for example.

    mock_tenacity_wait([1, 2], [3, 4], max=100)

    After all wait lists are exhausted, i.e. the mock simulated waiting for
    the specified periods, the mock will constantly simulate
    ``time.monotonic()`` to return the specified max time, typically making
    tenacity detect a timeout.

    Internally the mock needs to prefix each list of waiting periods with two
    additional entries [0, 0] which are used by ``tenacity.Retrying()`` to
    inititialize its start times in ``BaseRetrying.begin()`` and
    ``RetryCallState.__init__()``, see
    https://github.com/jd/tenacity/blob/main/tenacity/__init__.py.
    """

    def expand(wait_lists):
        for waits in wait_lists:
            yield from [0, 0] + waits

    durations = expand(wait_lists)

    def mock():
        try:
            return next(durations)
        except StopIteration:
            return max

    with patch("tenacity.time.sleep"):
        with patch("tenacity.time.monotonic", side_effect=mock):
            yield


@pytest.mark.parametrize(
    "schema, expected",
    [
        (None, r'"alias_manifest_[0-9]+"'),
        ("schema", r'"schema"\."alias_manifest_[0-9]+"'),
    ],
)
def test_udf_name(schema, expected):
    assert re.match(expected, _udf_name(schema, "alias"))


def test_create_script_failure(archive_bucket_path):
    create_script = Mock(side_effect=Exception("failed to create UDF script"))
    sim = Simulator(nodes=4, udf_results=[], create_script=create_script)
    with pytest.raises(Exception, match="failed to create UDF script") as ex:
        with mock_tenacity_wait([1]):
            sim.testee.verify_all_nodes("alias", "schema", archive_bucket_path)


def test_failure(archive_bucket_path):
    sim = Simulator(
        nodes=4,
        udf_results=[
            [[1, False]],
            [[1, False]],
            [[1, False]],
        ],
    )
    with pytest.raises(ExtractException) as ex:
        with mock_tenacity_wait([1], [2, 4]):
            sim.testee.verify_all_nodes("alias", "schema", archive_bucket_path)
    assert "1 of 4 nodes are still pending. IDs: [1]" == str(ex.value)


def test_success(archive_bucket_path):
    sim = Simulator(
        nodes=4,
        udf_results=[
            [[1, False], [2, False]],
            [[1, True], [2, False]],
            [[1, True], [2, True]],
        ],
    )
    with mock_tenacity_wait([1], [2, 4]):
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
    udf_results = [
        [[1, False], [2, False]],
        [[1, True], [2, False]],
        [[1, True], [2, True]],
    ]
    sim = Simulator(
        nodes=4,
        udf_results=udf_results,
        create_script=create_script,
    )
    with pytest.raises(ExtractException) as ex:
        with mock_tenacity_wait([1], [2, 4]):
            sim.testee.verify_all_nodes("alias", "schema", archive_bucket_path)
    assert "1 of 4 nodes are still pending. IDs: [2]" == str(ex.value)
