class TaskDefinitionError(SyntaxError):
    def __init__(self, message=None, errors=None):
        if not message:
            message = "Task must be defined inside the flow class."

        super().__init__(message)
        self.errors = errors


class TaskExecutionError(SyntaxError):
    def __init__(self, message=None, errors=None):
        if not message:
            message = "Task must be executed as a staticmethod."

        super().__init__(message)
        self.errors = errors
