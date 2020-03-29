from ..core.task_node import AbstractNode
from ..core.uniflow import Uniflow
from ..exceptions.errors import TaskDefinitionError, TaskExecutionError
from .constants import DecoratorOperation


class Task(object):

    def __init__(self, f=None, compute="batch", depends_on=[]):
        self.__f = f
        self.compute = compute
        self.__depends_on = depends_on
        self.__parents = []
        self.__children = []

    @property
    def name(self):
        return self.__f.__name__

    @property
    def dependencies(self):
        return self.__depends_on

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
                return self.__execute(*args, **kwargs)

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

    def __execute(self, *args: [object], **kwargs: {str: object}) -> object:
        print(f"Executing task {self.name}")
        return self.__f(*args, **kwargs)
