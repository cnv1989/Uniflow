import __main__
import textwrap

from aws_cdk import core
from aws_cdk import aws_lambda as lambda_
from pathlib import Path

from ..constants import LAMBDA_RUNTIME
from ..cdk.flow_requirements import FlowRequirements
from ..cdk.flow_code import FlowCode


class UniflowStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        self.__build_requirements_layer()
        self.__build_code_layer()

    @property
    def code_dir(self):
        return Path(__main__.__file__).relative_to(Path.cwd()).as_posix().split('/')[0]

    def add_lambda_for_task(self, python_path: str, method_name: str) -> None:
        module_name, class_name = python_path.rsplit(".", 1)

        code = f"""
        import importlib
        
        def handler(event, context):
            module = importlib.import_module("{module_name}")
            assert hasattr(module, "{class_name}"), "class {class_name} is not in {module_name}"
            cls = getattr(module, "{class_name}")
            return getattr(cls, "{method_name}")(event, context)  
        """
        lambda_.Function(
            self,
            method_name,
            layers=[self.requirements_layer, self.code_layer],
            runtime=LAMBDA_RUNTIME,
            code=lambda_.InlineCode(textwrap.dedent(code)),
            handler="index.handler"
        )

    def __build_requirements_layer(self):
        # The code that defines your stack goes here
        self.requirements_layer = lambda_.LayerVersion(
            self,
            "requirements",
            code=FlowRequirements()
        )

    def __build_code_layer(self):
        # The code that defines your stack goes here
        self.code_layer = lambda_.LayerVersion(
            self,
            "code",
            code=FlowCode(self.code_dir)
        )
