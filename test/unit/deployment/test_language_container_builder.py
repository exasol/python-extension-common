import re

from exasol.python_extension_common.deployment.language_container_builder import (
    copy_slc_flavor,
    LanguageContainerBuilder
)


def test_copy_slc_flavor(tmp_path):
    copy_slc_flavor(tmp_path)
    flavor_base = tmp_path / 'flavor_base'
    assert flavor_base.exists()
    assert len(list(flavor_base.rglob('*'))) == 8


def test_set_language_alias():
    language_alias = 'MY_PYTHON3'
    with LanguageContainerBuilder('test_container', language_alias) as container_builder:
        lang_def = container_builder.read_file('flavor_base/language_definition')
        expected_start = fr'^\s*{language_alias}\s*=\s*localzmq'
        assert re.search(expected_start, lang_def)
