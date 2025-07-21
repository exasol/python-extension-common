from contextlib import ExitStack
from pathlib import Path

import pytest
from exasol.slc.models.compression_strategy import CompressionStrategy

from test.utils.db_utils import assert_udf_running

from exasol.python_extension_common.deployment.language_container_builder import (
    LanguageContainerBuilder,
    find_path_backwards,
)


def test_prepare_flavor():
    project_directory = find_path_backwards("pyproject.toml", __file__).parent

    with LanguageContainerBuilder("test_container") as container_builder:
        container_builder.prepare_flavor(project_directory)
        assert container_builder.requirements_file.exists()
        assert container_builder.requirements_file.stat().st_size > 0
        assert container_builder.wheel_target.exists()
        assert container_builder.wheel_target.is_dir()
        assert any(container_builder.wheel_target.iterdir())


def test_prepare_flavor_extra():
    """Tests that requirements from multiple projects can be added together"""
    project_directory = find_path_backwards("pyproject.toml", __file__).parent
    dummy_req = "xyz\n"
    with LanguageContainerBuilder("test_container") as container_builder:
        container_builder.requirements_file.write_text(dummy_req)
        container_builder.prepare_flavor(project_directory)
        assert container_builder.requirements_file.exists()
        assert container_builder.requirements_file.stat().st_size > len(dummy_req)
        assert container_builder.requirements_file.read_text().startswith(dummy_req)

@pytest.mark.parametrize(
    "compression_strategy, expected_container_file_suffix",
    [
        (CompressionStrategy.GZIP, ".tar.gz"),
        (CompressionStrategy.NONE, ".tar"),
    ],
)
def test_language_container_builder(deployer_factory, db_schema, language_alias, compression_strategy, expected_container_file_suffix):
    project_directory = find_path_backwards("pyproject.toml", __file__).parent

    with (ExitStack() as stack):
        # Build the SLC
        container_builder = stack.enter_context(LanguageContainerBuilder("test_container"))
        container_builder.prepare_flavor(project_directory)
        export_result = container_builder.export(compression_strategy=compression_strategy)
        export_info = export_result.export_infos[str(container_builder.flavor_path)]["release"]

        container_file_path = Path(export_info.cache_file)
        assert str(container_file_path.suffix).endswith(expected_container_file_suffix), f"Expected container file suffix {expected_container_file_suffix} does not match {container_file_path}"

        # Deploy the SCL
        deployer = stack.enter_context(deployer_factory(create_test_schema=True))
        deployer.run(
            container_file=Path(container_file_path), alter_system=True, allow_override=True
        )

        # Verify that the deployed SLC works.
        assert_udf_running(deployer.pyexasol_connection, language_alias, db_schema)
