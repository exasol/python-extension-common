from __future__ import annotations
from typing import Callable
import shutil
import subprocess
import tempfile
from pathlib import Path
from importlib import resources

from exasol_integration_test_docker_environment.lib.docker.images.image_info import ImageInfo   # type: ignore
from exasol_script_languages_container_tool.lib import api            # type: ignore
from exasol_script_languages_container_tool.lib.tasks.export.export_containers import ExportContainerResult           # type: ignore


def exclude_cuda(line: str) -> bool:
    return not line.startswith("nvidia")


def find_file_or_folder_backwards(name: str) -> Path:
    current_path = Path(__file__).parent
    result_path = None
    while current_path != current_path.root:
        result_path = Path(current_path, name)
        if result_path.exists():
            break
        current_path = current_path.parent
    if result_path is not None and result_path.exists():
        return result_path
    else:
        raise RuntimeError(f"Could not find {name} when searching backwards from {Path(__file__).parent}")


def copy_slc_flavor(dest_dir: str | Path) -> None:
    files = resources.files(__package__).joinpath('language_container')
    with resources.as_file(files) as pkg_dir:
        shutil.copytree(pkg_dir, dest_dir, dirs_exist_ok=True)


class LanguageContainerBuilder:

    def __init__(self, container_name: str):
        self.container_name = container_name
        self._root_path: Path | None = None
        self._output_path: Path | None = None

    def __enter__(self):
        """
        Creates a standard flavor template in a temporary directory.

        Creates a temporary directory and the :container_name: subdirectory inside it.
        Copies all files from the standard flavor in there.
        """
        self._root_path = Path(tempfile.mkdtemp())
        self._output_path = self._root_path / '.output'
        self._output_path.mkdir()
        self.flavor_path = self._root_path / self.container_name
        copy_slc_flavor(self.flavor_path)
        return self

    def __exit__(self, *exc_details):

        # Delete all local docker images.
        if self._output_path is not None:
            api.clean_all_images(output_directory=str(self._output_path))
            self._output_path = None

        # Remove the temporary directory recursively
        if self._root_path is not None:
            shutil.rmtree(self._root_path, ignore_errors=True)
            self._root_path = None

    def read_file(self, file_name: str) -> str:
        """
        Reads the content of the specified file in the container directory.
        """
        return ''

    def write_file(self, file_name: str, content: str) -> None:
        """
        Replaces the content of the specified file in the container directory.
        This allows making modifications to the standard flavor.
        """

    @property
    def flavor_base(self):
        return self.flavor_path / "flavor_base"

    def prepare_flavor(self, project_directory: str | Path,
                       requirement_filter: Callable[[str], bool] | None = None):

        self._add_requirements_to_flavor(project_directory, requirement_filter)
        self._add_wheel_to_flavor(project_directory)

    def build(self) -> dict[str, ImageInfo]:
        """
        Builds the new script language container.
        """
        image_info = api.build(flavor_path=(str(self.flavor_path),), goal=("release",))
        return image_info

    def export(self, export_path: str | Path | None = None) -> ExportContainerResult:
        """
        Exports the container into an archive.
        """
        assert self._root_path is not None
        if not export_path:
            export_path = self._root_path / '.export'
            if not export_path.exists():
                export_path.mkdir()
        export_result = api.export(flavor_path=(str(self.flavor_path),),
                                   output_directory=str(self._output_path),
                                   export_path=str(export_path))
        return export_result

    def _add_requirements_to_flavor(self, project_directory: str | Path,
                                    requirement_filter: Callable[[str], bool] | None):

        assert self._root_path is not None
        dist_path = self._root_path / "requirements.txt"
        requirements_bytes = subprocess.check_output(["poetry", "export",
                                                      "--without-hashes", "--without-urls",
                                                      "--output", f'{dist_path}'],
                                                     cwd=str(project_directory))
        requirements = requirements_bytes.decode("UTF-8")
        if requirement_filter is not None:
            requirements = "\n".join(filter(requirement_filter, requirements.splitlines()))
        requirements_file = self.flavor_base / "dependencies" / "requirements.txt"
        requirements_file.write_text(requirements)

    def _add_wheel_to_flavor(self, project_directory: str | Path):

        assert self._root_path is not None
        # A newer version of poetry would allow using the --output parameter in
        # the build command. Then we could build the wheel in a temporary directory.
        # With the version currently in use we have to do this inside the project.
        dist_path = Path(project_directory) / "dist"
        if dist_path.exists():
            shutil.rmtree(dist_path)
        subprocess.call(["poetry", "build"], cwd=str(project_directory))
        wheels = list(dist_path.glob("*.whl"))
        if len(wheels) != 1:
            raise RuntimeError(f"Did not find exactly one wheel file in dist directory {dist_path}. "
                               f"Found the following wheels: {wheels}")
        wheel = wheels[0]
        wheel_target = self.flavor_base / "release" / "dist"
        wheel_target.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(wheel, wheel_target / wheel.name)
