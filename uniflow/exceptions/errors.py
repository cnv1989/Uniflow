class TaskDefinitionError(SyntaxError):
    def __init__(self, task, message=None, errors=None):
        if not message:
            message = "Task must be defined inside the flow class."

        message = f"Found invalid definition for task: {task.name}"

        super().__init__(message)
        self.errors = errors


class TaskExecutionError(RuntimeError):
    def __init__(self, task, message=None, errors=None):
        if not message:
            message = "Task must be executed as a staticmethod."

        super().__init__(message)
        self.errors = errors


class TaskCompilationError(RuntimeError):
    def __init__(self, task, message=None, errors=None):
        if not message:
            message = "Task must be compiled as a staticmethod."

        super().__init__(message)
        self.errors = errors
