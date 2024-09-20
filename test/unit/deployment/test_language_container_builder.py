import re
import pytest

from exasol.python_extension_common.deployment.language_container_builder import (
    copy_slc_flavor,
    LanguageContainerBuilder,
    find_path_backwards
)


def test_find_path_backwards(tmp_path):
    target_path = tmp_path / 'target.txt'
    target_path.write_text('content')
    start_dir = tmp_path / 'sub_dir'
    start_dir.mkdir()
    start_path = start_dir / 'start_file.txt'
    start_path.write_text('something')
    find_path_backwards(target_path.name, start_path)


def test_find_path_backwards_failure(tmp_path):
    start_dir = tmp_path / 'sub_dir'
    start_dir.mkdir()
    start_path = start_dir / 'start_file.txt'
    start_path.write_text('something')
    with pytest.raises(FileNotFoundError):
        find_path_backwards('ajf74096kdh3_9kdlk_jdheyshvle', start_path)


def test_copy_slc_flavor(tmp_path):
    copy_slc_flavor(tmp_path)
    flavor_base = tmp_path / 'flavor_base'
    assert flavor_base.exists()
    assert len(list(flavor_base.rglob('*'))) == 8
