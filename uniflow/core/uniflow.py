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
        self.__unigraph = None
        self.app = core.App(outdir=Path.cwd().joinpath("cdk.out").as_posix())
        self.stack = UniflowStack(self.app, self.__class__.__name__, self.code_dir, get_python_path(self))
        self.tasks = []

    @property
    def code_dir(self) -> Path:
        return Path.cwd().joinpath(self.__module__.split('.')[0])

    def __get_task_methods(self) -> Generator[Callable, None, None]:
        tasks = list(get_methods_with_decorator(self.__class__, "task"))
        for task in tasks:
            yield getattr(self, task)

    def __compile_all_tasks(self) -> None:
        self.tasks = []
        for task in self.__get_task_methods():
            self.tasks.append(task(compile=True))

    def __build_graph(self) -> None:
        from .unigraph import Unigraph  # avoid circular import task -> uniflow -> unigraph -> task
        self.__compile_all_tasks()
        self.__unigraph = Unigraph.from_tasks_list(self.tasks, self.stack)
        self.__unigraph.build()

    def build(self) -> None:
        self.__build_graph()
        self.app.synth()
