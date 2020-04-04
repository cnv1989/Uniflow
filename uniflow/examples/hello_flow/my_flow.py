from uniflow import Uniflow
from uniflow.decorators import task


class MyFlow(Uniflow):

    @task
    def task1(event, context):
        return "Task 1"

    @task(depends_on=["task1"])
    def task2(event, context):
        return "Task 2"

    @task(depends_on=["task2"])
    def task3(event, context):
        return "Task 3"

    @task
    def task4(event, context):
        return "Task 4"

    @task(depends_on=["task3", "task4"])
    def task5(event, context):
        return "Task 5"

    @task(depends_on=["task5"])
    def task6(event, context):
        return "Task 6"
