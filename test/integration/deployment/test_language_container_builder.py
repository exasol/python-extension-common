from contextlib import ExitStack
from pathlib import Path
from test.utils.db_utils import assert_udf_running

import pytest
from exasol.slc.models.compression_strategy import CompressionStrategy

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


@pytest.fixture(scope="session")
def language_container_builder():
    project_directory = find_path_backwards("pyproject.toml", __file__).parent

    with LanguageContainerBuilder("test_container") as container_builder:
        # Prepare the SLC
        container_builder.prepare_flavor(project_directory)
        yield container_builder


@pytest.fixture(
    scope="session",
    params=[
        (CompressionStrategy.GZIP, ".tar.gz"),
        (CompressionStrategy.NONE, ".tar"),
    ],
    ids=["with_gzip_compression", "no_compression"],
)
def slc_container_file_path(request, language_container_builder):
    compression_strategy = request.param[0]
    expected_container_file_suffix = request.param[1]

    export_result = language_container_builder.export(compression_strategy=compression_strategy)
    export_info = export_result.export_infos[str(language_container_builder.flavor_path)]["release"]
    container_file_path = Path(export_info.cache_file)
    assert container_file_path.name.endswith(
        expected_container_file_suffix
    ), f"Expected container file suffix {expected_container_file_suffix} does not match {container_file_path}"
    return container_file_path


def test_language_container_builder(
    language_container_builder, deployer_factory, db_schema, language_alias, slc_container_file_path
):
    with deployer_factory(db_schema) as deployer:
        deployer.run(
            container_file=Path(slc_container_file_path), alter_system=True, allow_override=True
        )

        # Verify that the deployed SLC works.
        assert_udf_running(deployer.pyexasol_connection, language_alias, db_schema)
