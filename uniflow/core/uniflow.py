import logging

from pathlib import Path
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

    @property
    def code_dir(self) -> Path:
        return Path.cwd().joinpath(self.__module__.split('.')[0])

    def __get_task_methods(self) -> object:
        tasks = list(get_methods_with_decorator(self.__class__, "task"))
        for task in tasks:
            yield getattr(self, task)

    def __build_graph(self) -> None:
        from .unigraph import Unigraph  # avoid circular import task -> uniflow -> unigraph -> task

        tasks = []
        for task in self.__get_task_methods():
            tasks.append(task(compile=True))
        self.__unigraph = Unigraph.from_tasks_list(tasks)
        self.__unigraph.build()

    def __create_task_lambdas(self):
        for node in self.__unigraph.nodes:
            self.stack.add_lambda_for_task(node.name)

    def build(self) -> None:
        self.__build_graph()
        self.__create_task_lambdas()
        self.app.synth()
