import logging

from uniflow import Uniflow
from uniflow.decorators import task

logger = logging.getLogger(__name__)


class MyFlow(Uniflow):

    @task
    def task1():
        return "Task 1"

    @task(depends_on=["task1"])
    def task2(task1):
        logger.info(f"Parent Task Results: task1={task1}")
        return "Task 2"

    @task(depends_on=["task2"])
    def task3(task2):
        return "Task 3"

    @task
    def task4():
        return "Task 4"

    @task(depends_on=["task3", "task4"])
    def task5(task3, task4):
        logger.info(f"Parent Task Results: task3={task3}, task4={task4}")
        return "Task 5"

    @task(depends_on=["task5"])
    def task6(task5):
        return "Task 6"
