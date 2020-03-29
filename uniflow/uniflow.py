from aws_cdk import core
from .stacks.core_stack import CoreStack
from .utils import methodsWithDecorator


class Uniflow(core.App):

    def __init__(self):
        super().__init__()
        self.core_stack = CoreStack(self, "core-stack")

    def __build_tasks(self):
        tasks = list(methodsWithDecorator(self.__class__, "task"))
        for task in tasks:
            getattr(self, task)(compile=True)

    def build(self):
        self.__build_tasks()


if __name__ == "__main__":
    Uniflow().synth()
