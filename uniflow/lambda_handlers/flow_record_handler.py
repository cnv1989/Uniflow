import os
import logging
from ..utils import get_flow_class_from_flow
from ..models.task_model import TaskModel

logger = logging.getLogger(__name__)

FlowClass = get_flow_class_from_flow(os.environ['FLOW_NAME'])


class FlowRecordHander(object):

    def __init__(self, record: dict) -> None:
        self.__record = record

    @property
    def event_type(self) -> str:
        return self.__record['eventName']

    @property
    def flow_id(self) -> str:
        return self.__record['dynamodb']['Keys']['FlowId']['S']

    def process(self) -> None:
        if self.event_type == 'INSERT':
            task_graph = FlowClass.generate_task_graph()
            node_and_task_tuple = []
            for node in task_graph.nodes:
                task = TaskModel.create_new_task_for_flow(self.flow_id, node)
                node.update_task_run_id(task.run_id)
                node_and_task_tuple.append((node, task))

            for (node, task) in node_and_task_tuple:
                task.update_run_ids(node)
