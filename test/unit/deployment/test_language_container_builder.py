from exasol.python_extension_common.deployment.language_container_builder import copy_slc_flavor


def test_copy_slc_flavor(tmp_path):
    copy_slc_flavor(tmp_path)
    flavor_base = tmp_path / 'flavor_base'
    assert flavor_base.exists()
    assert len(list(flavor_base.rglob('*'))) == 8
