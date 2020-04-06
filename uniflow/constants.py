import shutil
from aws_cdk import aws_lambda as lambda_
from enum import Enum, auto


LAMBDA_RUNTIME = lambda_.Runtime.PYTHON_3_7
IGNORE_PATTERNS = shutil.ignore_patterns('*.pyc', 'tmp*', 'cdk.out', '__pycache__', '*.egg-info', '.git')


class JobPriority(Enum):
    HIGH = 10
    MEDIUM = 6
    LOW = 3


class DecoratorOperation(Enum):
    COMPILE = auto()
    EXECUTE = auto()
