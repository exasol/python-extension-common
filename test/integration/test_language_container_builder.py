from typing import Callable
from contextlib import ExitStack
from pathlib import Path

from pyexasol import ExaConnection
from exasol.pytest_itde import config

from exasol.python_extension_common.deployment.language_container_builder import (
    LanguageContainerBuilder,
    find_path_backwards
)
from test.utils.revert_language_settings import revert_language_settings
from test.utils.db_utils import (create_schema, assert_udf_running)
from test.integration.test_language_container_deployer import (
    create_container_deployer, TEST_SCHEMA, TEST_LANGUAGE_ALIAS
)


def test_prepare_flavor(tmp_path):
    project_directory = find_path_backwards("pyproject.toml", __file__).parent

    with LanguageContainerBuilder('test_container', TEST_LANGUAGE_ALIAS) as container_builder:
        container_builder.prepare_flavor(project_directory)
        assert container_builder.requirements_file.exists()
        assert container_builder.requirements_file.stat().st_size > 0
        assert container_builder.wheel_target.exists()
        assert container_builder.wheel_target.is_dir()
        assert any(container_builder.wheel_target.iterdir())


def test_prepare_flavor_extra(tmp_path):
    """Tests that requirements from multiple projects can be added together"""
    project_directory = find_path_backwards("pyproject.toml", __file__).parent
    dummy_req = 'xyz\n'
    with LanguageContainerBuilder('test_container', TEST_LANGUAGE_ALIAS) as container_builder:
        container_builder.requirements_file.write_text(dummy_req)
        container_builder.prepare_flavor(project_directory)
        assert container_builder.requirements_file.exists()
        assert container_builder.requirements_file.stat().st_size > len(dummy_req)
        assert container_builder.requirements_file.read_text().startswith(dummy_req)


def test_language_container_builder(itde: config.TestConfig,
                                    connection_factory: Callable[[config.Exasol], ExaConnection],
                                    tmp_path):
    project_directory = find_path_backwards("pyproject.toml", __file__).parent

    with ExitStack() as stack:
        # Build the SLC
        container_builder = stack.enter_context(LanguageContainerBuilder(
            'test_container', TEST_LANGUAGE_ALIAS))
        container_builder.prepare_flavor(project_directory)
        export_result = container_builder.export()
        export_info = export_result.export_infos[str(container_builder.flavor_path)]["release"]

        container_file_path = Path(export_info.cache_file)

        # Deploy the SCL
        pyexasol_connection = stack.enter_context(connection_factory(itde.db))
        stack.enter_context(revert_language_settings(pyexasol_connection))
        create_schema(pyexasol_connection, TEST_SCHEMA)
        deployer = create_container_deployer(language_alias=TEST_LANGUAGE_ALIAS,
                                             pyexasol_connection=pyexasol_connection,
                                             bucketfs_config=itde.bucketfs)
        deployer.run(container_file=Path(container_file_path), alter_system=True, allow_override=True)

        # Verify that the deployed SLC works.
        assert_udf_running(pyexasol_connection, TEST_LANGUAGE_ALIAS, TEST_SCHEMA)
