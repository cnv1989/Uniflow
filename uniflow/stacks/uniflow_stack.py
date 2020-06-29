import textwrap

from aws_cdk import core
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_batch as batch_
from aws_cdk import aws_ec2 as ec2_
from aws_cdk import aws_ecs as ecs_
from aws_cdk import aws_ecr as ecr_
from aws_cdk import aws_s3 as s3_
from aws_cdk import aws_iam as iam_
from aws_cdk import aws_dynamodb as dynamodb_
from aws_cdk import aws_lambda_event_sources as lambda_event_sources_
from aws_cdk import aws_apigateway as apigateway_
from pathlib import Path

from ..constants import JobPriority
from uniflow.cdk import LAMBDA_RUNTIME
from ..cdk.flow_requirements import FlowRequirements
from ..cdk.flow_code import FlowCode
from uniflow.docker.batch_container_image import BatchContainerImage


class UniflowStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, code_dir: Path, flow_name: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        self.__id = id
        self.__code_dir = code_dir
        self.__flow_name = flow_name
        self.__vpc = None
        self.__requirements_layer = None
        self.__code_layer = None
        self.__batch_container_image = None
        self.__lambda_functions = []
        self.__batch_job_definitions = {}
        self.__task_table = None
        self.__flow_table = None
        self.__rest_api = None

        self.__create_vpc()
        self.__create_datastore_bucket()
        self.__create_requirements_layer()
        self.__create_code_layer()
        self.__create_batch_container_image()
        self.__create_batch_infrastructure()
        self.__create_task_executor_job_definition()
        self.__create_task_table()
        self.__add_lambda_to_handle_task_table_events()
        self.__create_flow_table()
        self.__add_lambda_to_handle_flow_table_events()
        self.__create_rest_api()

    @property
    def code_dir(self) -> str:
        return self.__code_dir.as_posix()

    @property
    def vpc(self):
        return self.__vpc

    @property
    def requirements_layer(self):
        return self.__requirements_layer

    @property
    def code_layer(self):
        return self.__code_layer

    @property
    def batch_container_image(self) -> BatchContainerImage:
        return self.__batch_container_image

    def __create_task_table(self) -> None:
        self.__task_table = dynamodb_.Table(
            self,
            f"{self.__id}_TaskTable",
            partition_key=dynamodb_.Attribute(name="TaskName", type=dynamodb_.AttributeType.STRING),
            sort_key=dynamodb_.Attribute(name="RunId", type=dynamodb_.AttributeType.STRING),
            billing_mode=dynamodb_.BillingMode.PAY_PER_REQUEST,
            stream=dynamodb_.StreamViewType.NEW_IMAGE
        )

        self.__task_table.add_global_secondary_index(
            partition_key=dynamodb_.Attribute(name="FlowId", type=dynamodb_.AttributeType.STRING),
            sort_key=dynamodb_.Attribute(name="RunId", type=dynamodb_.AttributeType.STRING),
            index_name="TaskTableGlobalIndexFlowIdRunId"
        )

    def __create_flow_table(self) -> None:
        self.__flow_table = dynamodb_.Table(
            self,
            f"{self.__id}_FlowTable",
            partition_key=dynamodb_.Attribute(name="FlowId", type=dynamodb_.AttributeType.STRING),
            sort_key=dynamodb_.Attribute(name="Created", type=dynamodb_.AttributeType.STRING),
            billing_mode=dynamodb_.BillingMode.PAY_PER_REQUEST,
            stream=dynamodb_.StreamViewType.NEW_IMAGE
        )

        self.__flow_table.add_local_secondary_index(
            sort_key=dynamodb_.Attribute(name="LastModified", type=dynamodb_.AttributeType.STRING),
            index_name="FlowTableLocalIndexLastUpdated"
        )

    def __add_lambda_to_handle_task_table_events(self) -> None:
        code = f"""
        from uniflow.lambda_handlers import TaskTableEventHandler
        
        def handler(event, context):
            handler = TaskTableEventHandler(event, context)
            return handler.execute()
        """
        lambda_function = lambda_.Function(
            self,
            f"{self.__id}_TaskTableEventHandler",
            layers=[self.__requirements_layer, self.__code_layer],
            runtime=LAMBDA_RUNTIME,
            code=lambda_.InlineCode(textwrap.dedent(code)),
            handler="index.handler",
            timeout=core.Duration.minutes(15)
        )
        self.__task_table.grant_read_write_data(lambda_function)
        lambda_function.add_event_source(lambda_event_sources_.DynamoEventSource(
            self.__task_table,
            starting_position=lambda_.StartingPosition.LATEST
        ))
        self.__lambda_functions.append(lambda_function)

    def __add_lambda_to_handle_flow_table_events(self) -> None:
        code = f"""
        from uniflow.lambda_handlers import FlowTableEventHandler
        
        def handler(event, context):
            handler = FlowTableEventHandler(event, context)
            return handler.execute()
        """
        lambda_function = lambda_.Function(
            self,
            f"{self.__id}_FlowTableEventHandler",
            layers=[self.__requirements_layer, self.__code_layer],
            runtime=LAMBDA_RUNTIME,
            code=lambda_.InlineCode(textwrap.dedent(code)),
            handler="index.handler",
            timeout=core.Duration.minutes(15)
        )
        self.__flow_table.grant_read_write_data(lambda_function)
        lambda_function.add_event_source(lambda_event_sources_.DynamoEventSource(
            self.__flow_table,
            starting_position=lambda_.StartingPosition.LATEST
        ))
        self.__lambda_functions.append(lambda_function)

    def __create_rest_api(self) -> None:
        code = f"""
        from flask_serverless import APIGWProxy
        from uniflow.api import app
        
        handler = APIGWProxy(app)
        """
        lambda_function = lambda_.Function(
            self,
            f"{self.__id}_ApiHandler",
            layers=[self.__requirements_layer, self.__code_layer],
            runtime=LAMBDA_RUNTIME,
            code=lambda_.InlineCode(textwrap.dedent(code)),
            handler="index.handler",
            timeout=core.Duration.minutes(15),
            environment={
                "FLOW_NAME": self.__id,
                "FLOW_TABLE": self.__flow_table.table_name,
                "TASK_TABLE": self.__task_table.table_name,
            }
        )
        lambda_function.role.add_to_policy(
            iam_.PolicyStatement(
                effect=iam_.Effect.ALLOW,
                resources=[
                    self.__flow_table.table_arn,
                    f"{self.__flow_table.table_arn}/index/*",
                    self.__task_table.table_arn,
                    f"{self.__task_table.table_arn}/index/*",
                ],
                actions=['dynamodb:*']

            )
        )
        # self.__flow_table.grant_read_write_data(lambda_function)
        # self.__task_table.grant_read_write_data(lambda_function)
        self.__rest_api = apigateway_.LambdaRestApi(
            self,
            f"{self.__id}_Api",
            handler=lambda_function,
            proxy=True,
        )

    def __create_task_executor_job_definition(self) -> None:
        container_image = ecs_.ContainerImage.from_ecr_repository(self.__ecr_repository)
        job_definition_container = batch_.JobDefinitionContainer(
            image=container_image,
            memory_limit_mib=1024*2,
            job_role=self.__job_definition_role,
            environment={
                "FLOW": self.__flow_name,
                "FLOW_DATASTORE": self.__datastore.bucket_name
            }
        )

        job_def_id = f"{self.__id}_BatchTaskExecutorJobDef"

        job_definition = batch_.JobDefinition(
            self,
            job_def_id,
            container=job_definition_container
        )
        self.__batch_job_definitions[job_def_id] = job_definition

    def __create_requirements_layer(self):
        # The code that defines your stack goes here
        self.__requirements_layer = lambda_.LayerVersion(
            self,
            "requirements",
            code=FlowRequirements()
        )

    def __create_code_layer(self):
        # The code that defines your stack goes here
        self.__code_layer = lambda_.LayerVersion(
            self,
            "code",
            code=FlowCode(self.code_dir)
        )

    def __create_batch_container_image(self):
        self.__batch_container_image = BatchContainerImage(repository_name=self.__flow_name)
        self.__batch_container_image.build()

    def __create_vpc(self):
        self.__vpc = ec2_.Vpc(self, f"{self.__id}Vpc", max_azs=3)

    def __create_datastore_bucket(self):
        self.__datastore = s3_.Bucket(self, f"{self.__id}_FlowDatastore",)

    def __create_batch_infrastructure(self):
        self.__compute_resources = batch_.ComputeResources(
            vpc=self.__vpc,
            bid_percentage=50
        )

        self.__job_definition_role = iam_.Role(
            self,
            f"{self.__id}_BatchJobDefinitionRole",
            assumed_by=iam_.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[iam_.ManagedPolicy.from_aws_managed_policy_name("AWSBatchFullAccess")]
        )
        self.__datastore.grant_read_write(self.__job_definition_role)

        self.__compute_env = batch_.ComputeEnvironment(
            self,
            f"{self.__id}_BatchComputeEnvironment",
            compute_resources=self.__compute_resources
        )

        self.__job_queue_compute_environment = batch_.JobQueueComputeEnvironment(
            compute_environment=self.__compute_env,
            order=1
        )

        self.__high_priority_job_queue = batch_.JobQueue(
            self,
            f"{self.__id}_BatchHighPriorityQueue",
            compute_environments=[self.__job_queue_compute_environment],
            priority=JobPriority.HIGH.value
        )

        self.__medium_priority_job_queue = batch_.JobQueue(
            self,
            f"{self.__id}_BatchMediumPriorityQueue",
            compute_environments=[self.__job_queue_compute_environment],
            priority=JobPriority.MEDIUM.value
        )

        self.__low_priority_job_queue = batch_.JobQueue(
            self,
            f"{self.__id}_BatchLowPriorityQueue",
            compute_environments=[self.__job_queue_compute_environment],
            priority=JobPriority.LOW.value
        )

        self.__ecr_repository = ecr_.Repository.from_repository_arn(
            self,
            f"{self.__id}_BatchImageRepository",
            self.batch_container_image.repository_arn
        )

