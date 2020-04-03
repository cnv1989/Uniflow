import os
import logging
import shutil

from pathlib import Path
from ..constants import LAMBDA_RUNTIME


logger = logging.getLogger(__name__)


class CodeBuilder(object):

    CODE = "code"

    def __init__(self, code_directory: str, build_directory: str) -> None:
        self.__code_directory = code_directory
        self.__build_directory = build_directory

    @property
    def code_directory(self):
        return Path(self.__code_directory).resolve()

    @property
    def build_directory(self):
        return Path(self.__build_directory).resolve()

    @property
    def build_code_directory(self):
        return self.build_directory.joinpath(self.CODE).resolve()

    @property
    def site_packages(self):
        return self.build_code_directory.joinpath(f"python/lib/{LAMBDA_RUNTIME.to_string()}/site-packages/{self.code_directory.name}")

    @property
    def code_archive(self):
        return self.build_directory.joinpath("code.zip")

    def __copy_code_sites_packages(self):
        shutil.copytree(self.code_directory, self.site_packages)

    def __archive_code(self):
        logger.info(f"Archiving requirements in {self.build_code_directory.as_posix()} to {self.code_archive.as_posix()}")
        shutil.make_archive(self.code_archive.as_posix().replace('.zip', ''), 'zip', self.build_code_directory.as_posix())
        if not self.code_archive.exists():
            raise Exception("Failed to archive code!")

    def package(self):
        self.__copy_code_sites_packages()
        self.__archive_code()
