import __main__
import textwrap

from aws_cdk import core
from aws_cdk import aws_lambda as lambda_
from pathlib import Path

from ..constants import LAMBDA_RUNTIME
from ..cdk.flow_requirements import FlowRequirements
from ..cdk.flow_code import FlowCode
from ..cdk.task_image_builder import TaskImageBuilder


class UniflowStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, code_dir: Path, flow_name: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        self._code_dir = code_dir
        self._flow_name = flow_name
        self.__build_requirements_layer()
        self.__build_code_layer()
        self.__build_task_image()

    @property
    def code_dir(self) -> str:
        return self._code_dir.as_posix()

    def add_lambda_for_task(self, method_name: str) -> None:
        module_name, class_name = self._flow_name.rsplit(".", 1)

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

    def __build_task_image(self):
        self.task_image = TaskImageBuilder(repository_name=self._flow_name)
        self.task_image.build()
