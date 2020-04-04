import shutil
from aws_cdk import aws_lambda as lambda_


LAMBDA_RUNTIME = lambda_.Runtime.PYTHON_3_7
IGNORE_PATTERNS = shutil.ignore_patterns('*.pyc', 'tmp*', 'cdk.out', '__pycache__', '*.egg-info', '.git')
