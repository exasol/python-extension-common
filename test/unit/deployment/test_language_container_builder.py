from unittest.mock import (
    MagicMock,
    call,
)

import pytest
from _pytest.monkeypatch import MonkeyPatch
from exasol.slc import api
from exasol.slc.models.compression_strategy import CompressionStrategy

from exasol.python_extension_common.deployment.language_container_builder import (
    LanguageContainerBuilder,
    copy_slc_flavor,
    find_path_backwards,
)


def test_find_path_backwards(tmp_path):
    target_path = tmp_path / "target.txt"
    target_path.write_text("content")
    start_dir = tmp_path / "sub_dir"
    start_dir.mkdir()
    start_path = start_dir / "start_file.txt"
    start_path.write_text("something")
    find_path_backwards(target_path.name, start_path)


def test_find_path_backwards_failure(tmp_path):
    start_dir = tmp_path / "sub_dir"
    start_dir.mkdir()
    start_path = start_dir / "start_file.txt"
    start_path.write_text("something")
    with pytest.raises(FileNotFoundError):
        find_path_backwards("ajf74096kdh3_9kdlk_jdheyshvle", start_path)


def test_copy_slc_flavor(tmp_path):
    copy_slc_flavor(tmp_path)
    flavor_base = tmp_path / "flavor_base"
    assert flavor_base.exists()
    assert len(list(flavor_base.rglob("*"))) == 7


@pytest.fixture
def mock_export(monkeypatch: MonkeyPatch) -> MagicMock:
    mock = MagicMock()
    monkeypatch.setattr(api, "export", mock)
    return mock


@pytest.mark.parametrize(
    "compression_strategy",
    [
        (CompressionStrategy.GZIP),
        (CompressionStrategy.NONE),
    ],
)
def test_export(mock_export, tmp_path, compression_strategy):
    project_directory = find_path_backwards("pyproject.toml", __file__).parent
    with LanguageContainerBuilder("test_container") as builder:
        builder.prepare_flavor(project_directory)
        builder.export(tmp_path, compression_strategy=compression_strategy)
        expected_calls = [
            call(
                flavor_path=(str(builder.flavor_path),),
                output_directory=str(builder._output_path),
                export_path=str(tmp_path),
                compression_strategy=compression_strategy,
            ),
        ]
        assert mock_export.call_args_list == expected_calls
