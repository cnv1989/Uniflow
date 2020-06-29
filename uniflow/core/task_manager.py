import os
import boto3
import logging
import pickle

from ..decorators.task import Task


logger = logging.getLogger(__name__)


class TaskManager(object):
    s3_resource = boto3.resource('s3')

    def __init__(self, task: Task, run_id: str, local: bool = False) -> None:
        self.__task = task
        self.__run_id = run_id
        self.local = local

    @property
    def task(self):
        return self.__task

    @property
    def run_id(self):
        return self.__run_id

    @property
    def datastore(self) -> str:
        return os.environ['FLOW_DATASTORE']

    @property
    def task_object(self) -> str:
        return f"{os.environ['FLOW']}/{self.task.name}/{self.__run_id}/result.pkl"

    def __get_s3_key_for_parent_task_result(self, task):
        return f"{os.environ['FLOW']}/{task}/{self.__run_id}/result.pkl"

    def __get_parent_task_result_from_s3(self, task: str) -> object:
        logger.info(f"Loading parent_task={task} output from datastore for run_id={self.__run_id}.")
        obj = self.s3_resource.Object(self.datastore, self.__get_s3_key_for_parent_task_result(task))
        return pickle.loads(obj.get()['Body'].read())

    def __get_parent_tasks_outputs(self) -> [object]:
        logger.info(f"Loading parent task outputs from datastore for run_id={self.__run_id}.")
        parent_results = []
        for parent in self.task.dependencies:
            parent_results.append(self.__get_parent_task_result_from_s3(parent))
        return parent_results

    def __save_task_output(self, ret) -> None:
        logger.info(f"Saving task={self.task.name} output to datastore for run_id={self.__run_id}.")
        pickle_buffer = pickle.dumps(ret)
        self.s3_resource.Object(self.datastore, self.task_object).put(Body=pickle_buffer)

    def execute_task(self) -> None:
        logger.info(f"Executing task={self.task.name}")
        args = self.__get_parent_tasks_outputs()
        ret = self.task.function(*args)
        if self.local:
            logger.info(ret)
        else:
            self.__save_task_output(ret)

    def get_task_status(self):
        pass
