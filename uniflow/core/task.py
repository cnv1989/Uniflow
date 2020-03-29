from ..uniflow import Uniflow
from ..exceptions.syntax_errors import TaskDefinitionError, TaskExecutionError


class Task(object):

    def __init__(self, f=None, compute="batch"):
        self.f = f
        self.compute = compute

    def __get_decorator(self, obj=None):

        def wrapped_f(*args, **kwargs):
            is_instance_of_uniflow = (len(args) > 0 and isinstance(args[0], Uniflow)) or \
                                     (obj and isinstance(obj, Uniflow))
            should_compile = kwargs.pop('compile', False)

            if is_instance_of_uniflow and should_compile:
                self.__compile()
            elif is_instance_of_uniflow and not should_compile:
                raise TaskExecutionError()
            elif should_compile:
                raise TaskDefinitionError()
            else:
                print(f"Executing task {self.f.__name__}")
                return self.f(*args, **kwargs)

        return wrapped_f

    def __get__(self, obj, objtype=None):
        return self.__get_decorator(obj)

    def __call__(self, f):
        self.f = f
        return self.__get_decorator()

    def __compile(self):
        print(f"Compiling task {self.f.__name__}")
