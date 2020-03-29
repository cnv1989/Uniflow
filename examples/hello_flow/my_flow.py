from uniflow import Flow
from uniflow.core import task


class MyFlow(Flow):

    @task
    def task1():
        pass


if __name__ == "__main__":
    flow = MyFlow()
    flow.build()
    MyFlow.task1()
