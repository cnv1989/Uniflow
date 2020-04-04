import os
import base64
import docker
import boto3
import shutil
from botocore.exceptions import ClientError
from ..constants import IGNORE_PATTERNS

from pathlib import Path


class TaskImageBuilder(object):

    def __init__(self, repository_name: str = "uniflow.task.images") -> None:
        self.__repository_name = repository_name
        self.__ecr_client = boto3.client('ecr')
        self.__init_docker_client()

    @property
    def docker_file_path(self) -> Path:
        return Path(__file__).parent.joinpath('./Dockerfile').resolve()


    @property
    def code_directory(self) -> Path:
        return Path.cwd()

    @property
    def build_directory(self) -> Path:
        return Path.cwd().joinpath("cdk.out/docker_build")

    @property
    def tag(self) -> str:
        return self.__repository_info["repositoryUri"]

    def __create_ecr_repository_if_not_exists(self):
        try:
            response = self.__ecr_client .describe_repositories(
                repositoryNames=[self.__repository_name]
            )
            repositories = response.get('repositories')
            self.__repository_info = repositories[0]
        except ClientError:
            response = self.__ecr_client .create_repository(
                repositoryName=self.__repository_name,
            )

            self.__repository_info = response['repository']

    def __init_docker_client(self):
        self.__docker_client = docker.from_env()
        token = self.__ecr_client.get_authorization_token()
        username, password = base64.b64decode(token['authorizationData'][0]['authorizationToken']).decode().split(':')
        registry = token['authorizationData'][0]['proxyEndpoint']
        self.__docker_client.login(username, password, registry=registry)

    def __build_docker_image(self):
        image, build_log_generator = self.__docker_client.images.build(
            path=self.build_directory.as_posix(),
            tag=self.tag
        )

        for log in build_log_generator:
            print(log)

        for log in self.__docker_client.images.push(self.tag, stream=True, decode=True):
            print(log)

    def __copy_code_build_directory(self):
        if self.build_directory.exists():
            shutil.rmtree(self.build_directory)
        shutil.copytree(self.code_directory, self.build_directory, ignore=IGNORE_PATTERNS)

    def __copy_docker_file_to_build_directory(self):
        shutil.copy(self.docker_file_path, self.build_directory)


    def build(self):
        self.__copy_code_build_directory()
        self.__copy_docker_file_to_build_directory()
        self.__create_ecr_repository_if_not_exists()
        self.__build_docker_image()
