import boto3
import os
import logging
import json
from ..utils import get_flow_class_from_flow
from ..models.task_model import TaskModel
from ..constants import TaskStatus

logger = logging.getLogger(__name__)

FLOW_CLASS = get_flow_class_from_flow(os.environ['FLOW_NAME'])
STATE_MACHINE_ARN = os.environ['TASK_EXECUTATION_STATE_MACHINE_ARN']
REGION = os.environ['AWS_REGION']
sfn_client = boto3.client('stepfunctions')


class TaskRecordHandler(object):
    def __init__(self, record: dict) -> None:
        self.__record = record

    @property
    def event_type(self) -> str:
        return self.__record['eventName']

    @property
    def flow_id(self) -> str:
        return self.__record['dynamodb']['NewImage']['FlowId']['S']

    @property
    def run_id(self) -> str:
        return self.__record['dynamodb']['NewImage']['RunId']['S']

    @property
    def task_name(self) -> str:
        return self.__record['dynamodb']['NewImage']['TaskName']['S']
        

    @property
    def execution_name(self) -> str:
        return f"{self.task_name}_{self.flow_id.split('-')[0]}_{self.run_id.split('-')[0]}"

    @property
    def sfn_input(self) -> dict:
        return {
            "flow_id": self.flow_id,
            "run_id": self.run_id,
            "task_name": self.task_name
        }

    def process(self) -> None:
        invoke_task_execution = False
        task = TaskModel.get_from_sfn_input(self.sfn_input)
        if self.event_type == 'INSERT' and task.parent_status == TaskStatus.COMPLETED.name:
            invoke_task_execution = True
        elif self.event_type == 'MODIFY':
            for child in task.child_tasks:
                if child.run_id:
                    child_task = TaskModel.get(child.task_name, child.run_id)
                    child_task.update_parent_task_status(task)

            for parent in task.parent_tasks:
                if  parent.run_id:
                    parent_task = TaskModel.get( parent.task_name,  parent.run_id)
                    parent_task.update_child_task_status(task)

            if task.status == TaskStatus.CREATED.name and task.parent_status == TaskStatus.COMPLETED.name:
                invoke_task_execution = True
    
        if invoke_task_execution:
            task.update_task_status(TaskStatus.PROGRESS)
            sfn_client.start_execution(
                stateMachineArn=STATE_MACHINE_ARN,
                name=self.execution_name,
                input=json.dumps(self.sfn_input)
            )
