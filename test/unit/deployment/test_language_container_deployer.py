from pathlib import (
    Path,
    PurePosixPath,
)
from unittest.mock import (
    MagicMock,
    Mock,
    call,
    create_autospec,
    patch,
)

import exasol.bucketfs as bfs
import pytest
from pyexasol import ExaConnection

from exasol.python_extension_common.deployment.extract_validator import ExtractValidator
from exasol.python_extension_common.deployment.language_container_deployer import (
    LanguageActivationLevel,
    LanguageContainerDeployer,
)


def bucket_path(path: str):
    bucket_api = bfs.MountedBucket("svc", "bkt")
    return bfs.path.BucketPath(path, bucket_api=bucket_api)


def equal(a: bfs.path.BucketPath, b: bfs.path.BucketPath) -> bool:
    return (a._path, a._bucket_api) == (b._path, b._bucket_api)


@pytest.fixture(scope="module")
def container_file_name() -> str:
    return "container_xyz.tar.gz"


@pytest.fixture(scope="module")
def container_file_path(container_file_name) -> Path:
    return Path(container_file_name)


@pytest.fixture
def container_file(tmp_path, container_file_name) -> Path:
    file = tmp_path / container_file_name
    file.touch()
    return file


@pytest.fixture(scope="module")
def language_alias() -> str:
    return "PYTHON3_TEST"


@pytest.fixture(scope="module")
def container_bfs_path(container_file_name) -> str:
    return f"bfsdefault/default/container/{container_file_name[:-7]}"


@pytest.fixture(scope="module")
def mock_pyexasol_conn() -> ExaConnection:
    return create_autospec(ExaConnection)


@pytest.fixture
def sample_bucket_path():
    return bucket_path("/")


@pytest.fixture
def container_deployer(
    mock_pyexasol_conn,
    language_alias,
    sample_bucket_path,
) -> LanguageContainerDeployer:
    deployer = LanguageContainerDeployer(
        pyexasol_connection=mock_pyexasol_conn,
        language_alias=language_alias,
        bucketfs_path=sample_bucket_path,
        extract_validator=Mock(),
    )
    deployer.upload_container = MagicMock()
    deployer.activate_container = MagicMock()
    return deployer


def test_slc_deployer_deploy(container_deployer, container_file_name, container_file_path):
    container_deployer.run(
        container_file=container_file_path,
        bucket_file_path=container_file_name,
        alter_system=True,
        allow_override=True,
        wait_for_completion=False,
    )
    container_deployer.upload_container.assert_called_once_with(
        container_file_path, container_file_name
    )
    expected_calls = [
        call(container_file_name, LanguageActivationLevel.Session, True),
        call(container_file_name, LanguageActivationLevel.System, True),
    ]
    container_deployer.activate_container.assert_has_calls(expected_calls, any_order=True)


def test_slc_deployer_upload(container_deployer, container_file_name, container_file_path):
    container_deployer.run(
        container_file=container_file_path, alter_system=False, wait_for_completion=False
    )
    container_deployer.upload_container.assert_called_once_with(
        container_file_path, container_file_name
    )
    container_deployer.activate_container.assert_called_once_with(
        container_file_name, LanguageActivationLevel.Session, False
    )


def test_slc_deployer_activate(container_deployer, container_file_name):
    container_deployer.run(
        bucket_file_path=container_file_name,
        alter_system=True,
        allow_override=True,
        wait_for_completion=False,
    )
    container_deployer.upload_container.assert_not_called()
    expected_calls = [
        call(container_file_name, LanguageActivationLevel.Session, True),
        call(container_file_name, LanguageActivationLevel.System, True),
    ]
    container_deployer.activate_container.assert_has_calls(expected_calls, any_order=True)


@patch("exasol.python_extension_common.deployment.language_container_deployer.get_udf_path")
@patch(
    "exasol.python_extension_common.deployment.language_container_deployer.get_language_settings"
)
def test_slc_deployer_generate_activation_command(
    mock_lang_settings,
    mock_udf_path,
    container_deployer,
    language_alias,
    container_file_name,
    container_bfs_path,
):
    mock_lang_settings.return_value = "R=builtin_r JAVA=builtin_java PYTHON3=builtin_python3"
    mock_udf_path.return_value = PurePosixPath(f"/buckets/{container_bfs_path}")

    alter_type = LanguageActivationLevel.Session
    expected_command = (
        f"ALTER {alter_type.value.upper()} SET SCRIPT_LANGUAGES='"
        "R=builtin_r JAVA=builtin_java PYTHON3=builtin_python3 "
        f"{language_alias}=localzmq+protobuf:///{container_bfs_path}?"
        f"lang=python#/buckets/{container_bfs_path}/exaudf/exaudfclient_py3';"
    )

    command = container_deployer.generate_activation_command(container_file_name, alter_type)
    assert command == expected_command


@patch("exasol.python_extension_common.deployment.language_container_deployer.get_udf_path")
@patch(
    "exasol.python_extension_common.deployment.language_container_deployer.get_language_settings"
)
def test_slc_deployer_generate_activation_command_override(
    mock_lang_settings,
    mock_udf_path,
    container_deployer,
    language_alias,
    container_file_name,
    container_bfs_path,
):
    current_bfs_path = "bfsdefault/default/container_abc"
    mock_lang_settings.return_value = (
        "R=builtin_r JAVA=builtin_java PYTHON3=builtin_python3 "
        f"{language_alias}=localzmq+protobuf:///{current_bfs_path}?"
        f"lang=python#/buckets/{current_bfs_path}/exaudf/exaudfclient_py3"
    )
    mock_udf_path.return_value = PurePosixPath(f"/buckets/{container_bfs_path}")

    alter_type = LanguageActivationLevel.Session
    expected_command = (
        f"ALTER {alter_type.value.upper()} SET SCRIPT_LANGUAGES='"
        "R=builtin_r JAVA=builtin_java PYTHON3=builtin_python3 "
        f"{language_alias}=localzmq+protobuf:///{container_bfs_path}?"
        f"lang=python#/buckets/{container_bfs_path}/exaudf/exaudfclient_py3';"
    )

    command = container_deployer.generate_activation_command(
        container_file_name, alter_type, allow_override=True
    )
    assert command == expected_command


@patch("exasol.python_extension_common.deployment.language_container_deployer.get_udf_path")
@patch(
    "exasol.python_extension_common.deployment.language_container_deployer.get_language_settings"
)
def test_slc_deployer_generate_activation_command_failure(
    mock_lang_settings,
    mock_udf_path,
    container_deployer,
    language_alias,
    container_file_name,
    container_bfs_path,
):
    current_bfs_path = "bfsdefault/default/container_abc"
    mock_lang_settings.return_value = (
        "R=builtin_r JAVA=builtin_java PYTHON3=builtin_python3 "
        f"{language_alias}=localzmq+protobuf:///{current_bfs_path}?"
        f"lang=python#/buckets/{current_bfs_path}/exaudf/exaudfclient_py3"
    )
    mock_udf_path.return_value = PurePosixPath(f"/buckets/{container_bfs_path}")

    with pytest.raises(RuntimeError):
        container_deployer.generate_activation_command(
            container_file_name, LanguageActivationLevel.Session, allow_override=False
        )


@patch("exasol.python_extension_common.deployment.language_container_deployer.get_udf_path")
def test_slc_deployer_get_language_definition(
    mock_udf_path, container_deployer, language_alias, container_file_name, container_bfs_path
):
    mock_udf_path.return_value = PurePosixPath(f"/buckets/{container_bfs_path}")
    expected_command = (
        f"{language_alias}=localzmq+protobuf:///{container_bfs_path}?"
        f"lang=python#/buckets/{container_bfs_path}/exaudf/exaudfclient_py3"
    )

    command = container_deployer.get_language_definition(container_file_name)
    assert command == expected_command


def test_extract_validator_called(sample_bucket_path, container_deployer, container_file):
    container_deployer.run(container_file, wait_for_completion=True)
    expected = container_deployer._extract_validator.verify_all_nodes
    assert expected.called and equal(
        expected.call_args.args[2], sample_bucket_path / container_file.name
    )


@pytest.mark.parametrize(
    "alter_system, print_activation_statements, invocation_generate_activation_command_expected",
    [
        (False, True, True),
        (True, True, False),
        (False, False, False),
        (True, False, False),
    ],
)
def test_print_alter_session_activation(
    container_deployer,
    container_file,
    alter_system,
    print_activation_statements,
    invocation_generate_activation_command_expected,
):
    container_deployer.generate_activation_command = MagicMock()
    container_deployer.run(
        container_file,
        wait_for_completion=True,
        alter_system=alter_system,
        print_activation_statements=print_activation_statements,
    )
    assert (
        container_deployer.generate_activation_command.called
        == invocation_generate_activation_command_expected
    )
