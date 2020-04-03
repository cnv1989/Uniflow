from aws_cdk.aws_lambda import AssetCode
from .code_builder import CodeBuilder


class FlowCode(AssetCode):

    def __init__(self, code: str, build_directory: str = "cdk.out") -> None:
        lambda_code = CodeBuilder(code, build_directory)
        lambda_code.package()
        super().__init__(lambda_code.code_archive.as_posix())

    @property
    def is_inline(self) -> bool:
        return False
