from .abstract_node import AbstractNode


class TaskNode(AbstractNode):

    def __init__(self, task: object) -> None:
        super().__init__()
        self.__task = task
        self.__run_id = None

    @property
    def name(self) -> str:
        return self.__task.name

    @property
    def task(self) -> object:
        return self.__task

    @property
    def run_id(self) -> str:
        return self.__run_id

    def update_task_run_id(self, run_id: str) -> dict:
        self.__run_id = run_id



