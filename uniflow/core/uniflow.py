import json
import logging

from pathlib import Path
from typing import Generator, Callable
from ..utils import get_methods_with_decorator, get_python_path


logger = logging.getLogger(__name__)


class Uniflow(object):

    def __init__(self) -> None:
        from aws_cdk import core
        from ..stacks.uniflow_stack import UniflowStack

        """
        aws_cdk depends on nodejs module aws-cdk, when we deploy we don't package node dependency as it is not required.
        This code path is only executed during synthesis. By locally importing we avoid breaking the lambda function.  
        """

        super().__init__()
        self.app = core.App(outdir=Path.cwd().joinpath("cdk.out").as_posix())
        self.stack = UniflowStack(self.app, self.__class__.__name__, self.code_dir, get_python_path(self))

    @property
    def code_dir(self) -> Path:
        return Path.cwd().joinpath(self.__module__.split('.')[0])

    @classmethod
    def list_task_name(cls):
        return list(get_methods_with_decorator(cls, "task"))

    @classmethod
    def compile_and_list_tasks(cls) -> Generator[Callable, None, None]:
        tasks = []
        task_names = list(cls.list_task_name())
        for task_name in task_names:
            task = getattr(cls, task_name)
            tasks.append(task(compile=True))
        return tasks

    @classmethod
    def generate_task_graph(cls) -> None:
        from .unigraph import Unigraph  # avoid circular import task -> uniflow -> unigraph -> task
        tasks = cls.compile_and_list_tasks()
        return Unigraph(tasks)

    def build(self) -> None:
        self.generate_task_graph()
        self.app.synth()
