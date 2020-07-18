import os
from uuid import uuid4
from datetime import datetime
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, UTCDateTimeAttribute, ListAttribute, MapAttribute
from ..constants import TaskStatus
from ..core.task_node import TaskNode


class TaskAttribute(MapAttribute):
    task_name = UnicodeAttribute(attr_name="TaskName")
    run_id = UnicodeAttribute(attr_name="RunId")
    status = UnicodeAttribute(attr_name="status")


class TaskModel(Model):

    class Meta:
        table_name = os.environ["TASK_TABLE"]
        region = os.environ["AWS_REGION"]

    task_name = UnicodeAttribute(hash_key=True, attr_name="TaskName")
    run_id = UnicodeAttribute(range_key=True, attr_name="RunId")
    flow_id = UnicodeAttribute(attr_name="FlowId")
    created = UTCDateTimeAttribute(attr_name="Created")
    status = UnicodeAttribute(attr_name="Status")
    parent_tasks = ListAttribute(attr_name="ParentTasks", of=TaskAttribute, null=True)
    child_tasks = ListAttribute(attr_name="ChildTasks", of=TaskAttribute, null=True)

    @classmethod
    def create_new_task_for_flow(cls, flow_id: str, task_node: TaskNode) -> object:
        now = datetime.utcnow()
        new_task = cls(
            task_name=task_node.name,
            run_id=str(uuid4()),
            flow_id=flow_id,
            created=now,
            status=TaskStatus.CREATED.name
        )

        new_task.save()
        return new_task
