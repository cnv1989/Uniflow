from aws_cdk import core
from ..stacks.core_stack import CoreStack
from ..utils import methodsWithDecorator


class Uniflow(core.App):

    def __init__(self):
        super().__init__()
        self.__unigraph = None
        self.core_stack = CoreStack(self, "core-stack")

    def __get_task_methods(self) -> object:
        tasks = list(methodsWithDecorator(self.__class__, "task"))
        for task in tasks:
            yield getattr(self, task)

    def __build_graph(self) -> None:
        from .unigraph import Unigraph  # avoid circular import task -> uniflow -> unigraph -> task

        tasks = []
        for task in self.__get_task_methods():
            tasks.append(task(compile=True))
        self.__unigraph = Unigraph.from_tasks_list(tasks)
        self.__unigraph.build()

    def build(self) -> None:
        self.__build_graph()


if __name__ == "__main__":
    Uniflow().synth()
