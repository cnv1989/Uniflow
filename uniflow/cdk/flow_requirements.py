from aws_cdk.aws_lambda import AssetCode
from .requirements_builder import RequirementsBuilder


class FlowRequirements(AssetCode):

    def __init__(self, requirements: str = "requirements.txt", build_directory: str = "cdk.out") -> None:
        lambda_reqs = RequirementsBuilder(requirements, build_directory)
        lambda_reqs.package()
        super().__init__(lambda_reqs.requirements_archive.as_posix())

    @property
    def is_inline(self) -> bool:
        return False
