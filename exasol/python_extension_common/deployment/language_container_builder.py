from __future__ import annotations
from typing import Callable
import shutil
import subprocess
import tempfile
from pathlib import Path
from importlib import resources

from exasol_integration_test_docker_environment.lib.docker.images.image_info import ImageInfo   # type: ignore
from exasol.slc import api            # type: ignore
from exasol.slc.models.export_container_result import ExportContainerResult     # type: ignore


def exclude_cuda(line: str) -> bool:
    return not line.startswith("nvidia")


def find_path_backwards(target_path: str | Path, start_path: str | Path) -> Path:
    """
    An utility searching for a specified path backwards. It begins with the given start
    path and checks if the target path is among its siblings. Then it moves to the parent
    path and so on, until it reaches the root of the file structure. Raises a FileNotFound
    error if the search is unsuccessful.
    """
    current_path = Path(start_path).parent
    root = Path(current_path.root)
    while current_path != root:
        result_path = Path(current_path, target_path)
        if result_path.exists():
            return result_path
        current_path = current_path.parent
    raise FileNotFoundError(f"Could not find {target_path} when searching backwards from {start_path}")


def copy_slc_flavor(dest_dir: str | Path) -> None:
    """
    Copies the content of the language_container directory to the specified
    destination directory.
    """
    files = resources.files(__package__).joinpath('language_container')
    with resources.as_file(files) as pkg_dir:
        shutil.copytree(pkg_dir, dest_dir, dirs_exist_ok=True)


class LanguageContainerBuilder:

    def __init__(self, container_name: str):
        self.container_name = container_name
        self._root_path: Path | None = None
        self._output_path: Path | None = None

    def __enter__(self):

        # Create a temporary working directory
        self._root_path = Path(tempfile.mkdtemp())
        self.flavor_path = self._root_path / self.container_name

        # Copy the flavor into the working directory
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

    def read_file(self, file_name: str | Path) -> str:
        """
        Reads the content of the specified file in the flavor directory.
        The provided file name should be relative to the flavor directory, e.g.
        flavor_base/dependencies/Dockerfile
        """
        file_path = self.flavor_path.joinpath(file_name)
        return file_path.read_text()

    def write_file(self, file_name: str | Path, content: str) -> None:
        """
        Replaces the content of the specified file in the flavor directory.
        This allows making modifications to the standard flavor.
        The provided file name should be relative to the flavor directory, e.g.
        flavor_base/dependencies/Dockerfile
        """
        file_path = self.flavor_path.joinpath(file_name)
        file_path.write_text(content)

    @property
    def flavor_base(self) -> Path:
        return self.flavor_path / "flavor_base"

    @property
    def requirements_file(self) -> Path:
        return self.flavor_base / "dependencies" / "requirements.txt"

    @property
    def wheel_target(self) -> Path:
        return self.flavor_base / "release" / "dist"

    def prepare_flavor(self, project_directory: str | Path,
                       requirement_filter: Callable[[str], bool] | None = None):
        """
        Create the project's requirements.txt and the distribution wheel.
        """
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
        if self._output_path is None:
            self._output_path = self._root_path / '.output'
            if not self._output_path.exists():
                self._output_path.mkdir()

        export_result = api.export(flavor_path=(str(self.flavor_path),),
                                   output_directory=str(self._output_path),
                                   export_path=str(export_path))
        return export_result

    def _add_requirements_to_flavor(self, project_directory: str | Path,
                                    requirement_filter: Callable[[str], bool] | None):
        """
        Adds project's requirements to the requirements.txt file. Creates this file
        if it doesn't exist.
        """
        assert self._root_path is not None
        requirements_bytes = subprocess.check_output(["poetry", "export",
                                                      "--without-hashes", "--without-urls"],
                                                     cwd=str(project_directory))
        requirements = requirements_bytes.decode("UTF-8")
        if requirement_filter is not None:
            requirements = "\n".join(filter(requirement_filter, requirements.splitlines()))
        # Make sure the content ends with a new line, so that other requirements can be
        # added at the end of it.
        if not requirements.endswith('\n'):
            requirements += '\n'
        with self.requirements_file.open(mode='a') as f:
            return f.write(requirements)

    def _add_wheel_to_flavor(self, project_directory: str | Path):
        """
        Create the project's distribution wheel.
        """
        assert self._root_path is not None
        # A newer version of poetry would allow using the --output parameter in
        # the build command. Then we could build the wheel in a temporary directory.
        # With the version currently used in the Python Toolbox we have to do this
        # inside the project.
        dist_path = Path(project_directory) / "dist"
        if dist_path.exists():
            shutil.rmtree(dist_path)
        subprocess.call(["poetry", "build"], cwd=str(project_directory))
        wheels = list(dist_path.glob("*.whl"))
        if len(wheels) != 1:
            raise RuntimeError(f"Did not find exactly one wheel file in dist directory {dist_path}. "
                               f"Found the following wheels: {wheels}")
        wheel = wheels[0]
        self.wheel_target.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(wheel, self.wheel_target / wheel.name)
