from exasol.slc.internal.tasks.build.docker_flavor_image_task import DockerFlavorAnalyzeImageTask    # type: ignore


class AnalyzeDependencies(DockerFlavorAnalyzeImageTask):
    def get_build_step(self) -> str:
        return "dependencies"

    def requires_tasks(self):
        return {}

    def get_path_in_flavor(self):
        return "flavor_base"


class AnalyzeRelease(DockerFlavorAnalyzeImageTask):
    def get_build_step(self) -> str:
        return "release"

    def requires_tasks(self):
        return {"dependencies":  AnalyzeDependencies}

    def get_path_in_flavor(self):
        return "flavor_base"
