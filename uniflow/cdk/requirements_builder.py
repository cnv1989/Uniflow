import os
import docker
import logging
import shutil

from pathlib import Path
from ..constants import LAMBDA_RUNTIME


logger = logging.getLogger(__name__)


class RequirementsBuilder(object):
    """
    https://gitlab.com/josef.stach/aws-cdk-lambda-asset/-/blob/master/aws_cdk_lambda_asset/zip_asset_code.py
    """

    DOCKER_MOUNT_DIRECTORY = Path("/opt/amazon")
    REQUIREMENTS = "requirements"

    def __init__(self, requirements: str, build_directory: str) -> None:
        self._requirements = requirements
        self._build_directory = build_directory

    @property
    def requirements(self):
        return Path(self._requirements).resolve().relative_to(Path.cwd())

    @property
    def build_directory(self):
        return Path(self._build_directory).resolve().relative_to(Path.cwd())

    @property
    def requirements_directory(self):
        return self.build_directory.joinpath(f"{self.REQUIREMENTS}")

    @property
    def site_packages(self):
        return self.requirements_directory.joinpath(f"python/lib/{LAMBDA_RUNTIME.to_string()}/site-packages/")

    @property
    def requirements_archive(self):
        return self.build_directory.joinpath(f"{self.REQUIREMENTS}.zip")

    def __create_build_directory_if_not_exist(self):
        if not self.requirements_directory.exists():
            os.makedirs(self.requirements_directory)

    def get_requirements_dir_path_inside_container(self):
        return self.DOCKER_MOUNT_DIRECTORY.joinpath(self.site_packages)

    def get_requirements_file_path_inside_container(self):
        return self.DOCKER_MOUNT_DIRECTORY.joinpath(self.requirements)

    def __build_in_docker(self) -> None:
        """
        Build lambda dependencies in a container as-close-as-possible to the actual runtime environment.
        """
        logger.warning('Installing dependencies [running in Docker]...')
        client = docker.from_env()
        container = client.containers.run(
            image='lambci/lambda:build-python3.7',
            command=f"""
                /bin/sh -c 'python3.7 -m pip install --target {self.get_requirements_dir_path_inside_container().as_posix()} --requirement {self.get_requirements_file_path_inside_container().as_posix()} &&
                find {self.get_requirements_dir_path_inside_container().as_posix()} -name \\*.so -exec strip \\{{\\}} \\;
            '""",
            remove=True,
            volumes={
                os.getcwd(): {
                    'bind': self.DOCKER_MOUNT_DIRECTORY.as_posix(),
                    'mode': 'rw'
                }
            },
            user=0,
            detach=True
        )

        for line in container.logs(stream=True):
            logger.info(line.decode("utf-8").strip())

    def __archive_requirements(self) -> None:
        if not self.requirements_directory.exists() or os.listdir(self.requirements_directory) == 0:
            raise Exception("Docker failed to build requirements!")

        logger.warning(f"Archiving requirements in {self.requirements_directory.as_posix()} to {self.requirements_archive.as_posix()}")
        shutil.make_archive(self.requirements_archive.as_posix().replace('.zip', ''), 'zip', self.requirements_directory.as_posix())
        if not self.requirements_archive.exists():
            raise Exception("Failed to archive requirements!")

    def package(self):
        self.__create_build_directory_if_not_exist()
        self.__build_in_docker()
        self.__archive_requirements()
