import os
import logging
import boto3
import pickle

from ..core.uniflow import Uniflow
from ..exceptions.errors import TaskDefinitionError, TaskExecutionError
from ..constants import DecoratorOperation, JobPriority


logger = logging.getLogger(__name__)


class Task(object):

    def __init__(self, f=None, compute="batch", depends_on=[]):
        self.__f = f
        self.compute = compute
        self.__run_id = None
        self.__priority = JobPriority.HIGH
        self.__depends_on = depends_on
        self.__parents = []
        self.__children = []

    @property
    def name(self):
        return self.__f.__name__

    @property
    def dependencies(self):
        return self.__depends_on

    @property
    def datastore(self) -> str:
        return os.environ['FLOW_DATASTORE']

    @property
    def task_object(self) -> str:
        return f"{os.environ['FLOW']}/{os.environ['TASK']}/{self.__run_id}/result.pkl"

    def __infer_decorator_operation_from_args(self, obj: object, *args: [object], **kwargs: {str: object}):
        is_instance_of_uniflow = (len(args) > 0 and isinstance(args[0], Uniflow)) or \
                                 (obj and isinstance(obj, Uniflow))
        should_compile = kwargs.pop('compile', False)

        if is_instance_of_uniflow and should_compile:
            return DecoratorOperation.COMPILE
        elif not is_instance_of_uniflow and not should_compile:
            return DecoratorOperation.EXECUTE
        elif is_instance_of_uniflow and not should_compile:
            raise TaskExecutionError(self)
        else:
            raise TaskDefinitionError(self)

    def __get_decorator(self, obj: object = None):

        self.__validate_dependency()

        def wrapped_f(*args, **kwargs):

            op = self.__infer_decorator_operation_from_args(obj, *args, **kwargs)

            if op == DecoratorOperation.COMPILE:
                return self.__compile()
            elif op == DecoratorOperation.EXECUTE:
                self.__run_id = kwargs.pop('run_id')
                self.__execute(**kwargs)

        return wrapped_f

    def __get__(self, obj: Uniflow, objtype: object = None):
        return self.__get_decorator(obj)

    def __call__(self, f):
        self.__f = f
        return self.__get_decorator()

    def __validate_dependency(self):
        if self.__depends_on and self.name in self.__depends_on:
            raise TaskDefinitionError(self, "Task cannot depend on itself.")

    def __compile(self):
        print(f"Compiling task {self.name}")
        return self

    def __execute(self) -> None:
        print(f"Executing task {self.name}")
        args = self.__get_parent_tasks_outputs()
        ret = self.__f(*args)
        self.__save_task_output(ret)

    def __get_parent_tasks_outputs(self) -> [object]:
        logger.info(f"Loading parent task outputs from datastore for run_id={self.__run_id}.")
        return []

    def __save_task_output(self, ret) -> None:
        logger.info(f"Saving task output to datastore for run_id={self.__run_id}.")
        pickle_buffer = pickle.dumps(ret)
        s3_resource = boto3.resource('s3')
        s3_resource.Object(self.datastore, self.task_object).put(Body=pickle_buffer)
