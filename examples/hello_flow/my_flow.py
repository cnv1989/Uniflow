from uniflow import Uniflow
from uniflow.decorators import task


class MyFlow(Uniflow):

    @task
    def task1():
        pass

    @task(depends_on=["task1"])
    def task2():
        pass

    @task(depends_on=["task2"])
    def task3():
        pass

    @task
    def task4():
        pass

    @task(depends_on=["task4"])
    def task5():
        pass

    @task(depends_on=["task5"])
    def task6():
        pass


if __name__ == "__main__":
    flow = MyFlow()
    flow.build()
    MyFlow.task1()
