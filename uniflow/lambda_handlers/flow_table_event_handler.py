import os
import logging
from ..utils import get_flow_class_from_flow
from ..models.task_model import TaskModel

logger = logging.getLogger(__name__)

FlowClass = get_flow_class_from_flow(os.environ['FLOW_NAME'])


class FlowTableEventHandler(object):

    def __init__(self, event: dict, context: dict) -> None:
        self.__event = event
        self.__context = context

    @property
    def event_type(self) -> str:
        return self.__event['Records'][0]['eventName']

    @property
    def flow_id(self) -> str:
        return self.__event['Records'][0]['dynamodb']['Keys']['FlowId']['S']

    def execute(self) -> None:
        if self.event_type == 'INSERT':
            task_graph = FlowClass.generate_task_graph()
            for node in task_graph.nodes:
                TaskModel.create_new_task_for_flow(self.flow_id, node)


