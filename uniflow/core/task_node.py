from .abstract_node import AbstractNode


class TaskNode(AbstractNode):

    def __init__(self, task: object) -> None:
        super().__init__()
        self.task = task

    @property
    def name(self):
        return self.task.name

