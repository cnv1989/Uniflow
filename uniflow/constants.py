import os
import shutil
from enum import Enum, auto


IGNORE_PATTERNS = shutil.ignore_patterns('*.pyc', 'tmp*', 'cdk.out', '__pycache__', '*.egg-info', '.git')


class JobPriority(Enum):
    HIGH = 10
    MEDIUM = 6
    LOW = 3


class DecoratorMode(Enum):
    COMPILATION = auto()
    EXECUTION = auto()
