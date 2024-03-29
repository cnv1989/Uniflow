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
from aws_cdk import aws_stepfunctions as sfn_
from aws_cdk import aws_stepfunctions_tasks as sfn_tasks_
from pathlib import Path


from uniflow.cdk import LAMBDA_RUNTIME
from uniflow.docker.batch_container_image import BatchContainerImage
from ..constants import JobPriority
from ..cdk.flow_requirements import FlowRequirements
from ..cdk.flow_code import FlowCode


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
        self.__create_task_table()
        self.__create_flow_table()
        self.__create_task_executor_job_definition()
        self.__create_state_machine()
        self.__add_lambda_to_handle_task_table_events()
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

        self.__task_table.add_local_secondary_index(
            sort_key=dynamodb_.Attribute(name="Created", type=dynamodb_.AttributeType.STRING),
            index_name="TaskTableLocalIndexCreated"
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

    def __add_iam_policy_to_lambda_function(self, lambda_function: lambda_.Function) -> None:
        self.__add_ddb_policy_to_lambda_function(lambda_function)
        self.__add_sfn_policy_to_lambda_function(lambda_function)

    def __add_ddb_policy_to_lambda_function(self, lambda_function: lambda_.Function) -> None:
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

    def __add_sfn_policy_to_lambda_function(self, lambda_function: lambda_.Function) -> None:
        lambda_function.role.add_to_policy(
            iam_.PolicyStatement(
                effect=iam_.Effect.ALLOW,
                resources=[
                    self.__state_machine.state_machine_arn,
                ],
                actions=['states:*']
            )
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
            timeout=core.Duration.minutes(15),
            environment={
                "FLOW_NAME": self.__flow_name,
                "FLOW_TABLE": self.__flow_table.table_name,
                "TASK_TABLE": self.__task_table.table_name,
                "TASK_EXECUTATION_STATE_MACHINE_ARN": self.__state_machine.state_machine_arn
            }
        )
        self.__add_iam_policy_to_lambda_function(lambda_function)
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
            timeout=core.Duration.minutes(15),
            environment={
                "FLOW_NAME": self.__flow_name,
                "FLOW_TABLE": self.__flow_table.table_name,
                "TASK_TABLE": self.__task_table.table_name,
                "TASK_EXECUTATION_STATE_MACHINE_ARN": self.__state_machine.state_machine_arn
            }
        )
        self.__add_iam_policy_to_lambda_function(lambda_function)
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
                "FLOW_NAME": self.__flow_name,
                "FLOW_TABLE": self.__flow_table.table_name,
                "TASK_TABLE": self.__task_table.table_name,
            }
        )
        self.__add_iam_policy_to_lambda_function(lambda_function)
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
                "FLOW_DATASTORE": self.__datastore.bucket_name,
                "FLOW_TABLE": self.__flow_table.table_name,
                "TASK_TABLE": self.__task_table.table_name,
                "AWS_REGION": self.region
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
            managed_policies=[
                iam_.ManagedPolicy.from_aws_managed_policy_name("AWSBatchFullAccess"),
                iam_.ManagedPolicy.from_aws_managed_policy_name("AmazonDynamoDBFullAccess")
            ]
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

    def __create_get_task_status_step(self) -> sfn_.Task:
        code = f"""
        from uniflow.models.task_model import TaskModel
        
        def handler(event, context):
            task = TaskModel.get_from_sfn_input(event)
            return task.get_status()
        """
        lambda_function = lambda_.Function(
            self,
            f"{self.__id}_GetTaskStatus",
            layers=[self.__requirements_layer, self.__code_layer],
            runtime=LAMBDA_RUNTIME,
            code=lambda_.InlineCode(textwrap.dedent(code)),
            handler="index.handler",
            timeout=core.Duration.minutes(15),
            environment={
                "FLOW_NAME": self.__flow_name,
                "FLOW_TABLE": self.__flow_table.table_name,
                "TASK_TABLE": self.__task_table.table_name
            }
        )
        self.__lambda_functions.append(lambda_function)
        self.__add_ddb_policy_to_lambda_function(lambda_function)

        self.__get_task_status_step = sfn_tasks_.LambdaInvoke(
            self,
            f"{self.__id}GetTaskStatus",
            lambda_function=lambda_function,
            result_path="$.TaskStatus"
        )

    def __create_task_executor_step(self) -> sfn_.Task:
        self.__task_executor_step = sfn_tasks_.BatchSubmitJob(
            self,
            f"{self.__id}SubmitBatchJob",
            job_definition=self.__batch_job_definitions[f"{self.__id}_BatchTaskExecutorJobDef"],
            job_queue=self.__high_priority_job_queue,
            job_name="TaskExecutor",
            payload=sfn_.TaskInput.from_object({
                "task_name.$": "$.task_name",
                "run_id.$": "$.run_id",
                "flow_id.$": "$.flow_id"
            }),
            container_overrides=sfn_tasks_.BatchContainerOverrides(
                command=["uniflow", "execute", "--task", "Ref::task_name", "--flow-id", "Ref::flow_id",  "--run-id", "Ref::run_id"]
            ),
            result_path="$.TaskExecutionResult"
        ).next(
            self.__task_completed_step
        )

    def __create_check_task_status_step(self) -> None:
        self.__check_task_status_step = sfn_.Choice(
            self,
            f"{self.__id}CheckTaskParentStatus"
        ).when(
            sfn_.Condition.string_equals('$.TaskStatus.Payload.parent_status', 'COMPLETED'),
            self.__task_executor_step
        ).when(
            sfn_.Condition.string_equals('$.TaskStatus.Payload.parent_status', 'PENDING'),
            sfn_.Wait(
                self,
                f"{self.__id}WaitForParentTasksToComplete",
                time=sfn_.WaitTime.duration(core.Duration.minutes(1))
            ).next(
                self.__get_task_status_step
            )
        ).otherwise(
            self.__task_failed_step
        )

    def __create_task_completed_step(self) -> sfn_.Task:
        code = f"""
        from uniflow.models.task_model import TaskModel
        from uniflow.constants import TaskStatus
        
        def handler(event, context):
            task = TaskModel.get_from_sfn_input(event)
            return task.update_task_status(TaskStatus.COMPLETED)
        """
        lambda_function = lambda_.Function(
            self,
            f"{self.__id}_TaskCompleted",
            layers=[self.__requirements_layer, self.__code_layer],
            runtime=LAMBDA_RUNTIME,
            code=lambda_.InlineCode(textwrap.dedent(code)),
            handler="index.handler",
            timeout=core.Duration.minutes(15),
            environment={
                "FLOW_NAME": self.__flow_name,
                "FLOW_TABLE": self.__flow_table.table_name,
                "TASK_TABLE": self.__task_table.table_name
            }
        )
        self.__lambda_functions.append(lambda_function)
        self.__add_ddb_policy_to_lambda_function(lambda_function)

        self.__task_completed_step = sfn_tasks_.LambdaInvoke(
            self,
            f"{self.__id}TaskCompleted",
            lambda_function=lambda_function,
            result_path="$.TaskCompleted"
        )

    def __create_task_failed_step(self) -> sfn_.Task:
        code = f"""
        from uniflow.models.task_model import TaskModel
        from uniflow.constants import TaskStatus
        
        def handler(event, context):
            task = TaskModel.get_from_sfn_input(event)
            return task.update_task_status(TaskStatus.FAILED)
        """
        lambda_function = lambda_.Function(
            self,
            f"{self.__id}_TaskFailed",
            layers=[self.__requirements_layer, self.__code_layer],
            runtime=LAMBDA_RUNTIME,
            code=lambda_.InlineCode(textwrap.dedent(code)),
            handler="index.handler",
            timeout=core.Duration.minutes(15),
            environment={
                "FLOW_NAME": self.__flow_name,
                "FLOW_TABLE": self.__flow_table.table_name,
                "TASK_TABLE": self.__task_table.table_name
            }
        )
        self.__lambda_functions.append(lambda_function)
        self.__add_ddb_policy_to_lambda_function(lambda_function)

        self.__task_failed_step = sfn_tasks_.LambdaInvoke(
            self,
            f"{self.__id}TaskFailed",
            lambda_function=lambda_function,
            result_path="$.TaskFailed"
        )

    def __create_state_machine(self) -> sfn_.StateMachine:
        self.__create_get_task_status_step()
        self.__create_task_completed_step()
        self.__create_task_failed_step()
        self.__create_task_executor_step()
        self.__create_check_task_status_step()

        definition = self.__get_task_status_step\
            .next(self.__check_task_status_step)

        self.__state_machine = sfn_.StateMachine(
            self,
            id=self.__id,
            definition=definition,
            timeout=core.Duration.hours(24)
        )
