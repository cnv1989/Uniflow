import logging
import numpy as np
from uniflow import Uniflow
from uniflow.decorators import task

logger = logging.getLogger(__name__)


class MyFlow(Uniflow):

    @task
    def init_numpy_array():
        return np.random.rand(3, 2)

    @task(depends_on=["init_numpy_array"])
    def multiply_by_10(numpy_array):
        return numpy_array*10
        
    @task
    def init_another_numpy_array():
        return np.random.rand(3, 2)

    @task(depends_on=["init_another_numpy_array"])
    def square_matrix(arr):
        return arr*arr

    @task(depends_on=["multiply_by_10", "init_another_numpy_array", "square_matrix"])
    def add_all_arrays(arr1, arr2, arr3):
        return arr1 + arr2 + arr3

    @task(depends_on=["add_all_arrays"])
    def divide_by_10(arr):
        return arr/10
