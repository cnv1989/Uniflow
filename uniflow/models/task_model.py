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

    @property
    def parent_status(self) -> str:
        for parent_task in self.parent_tasks:
            if parent_task.status != TaskStatus.COMPLETED.name:
                return TaskStatus.PENDING.name
        return TaskStatus.COMPLETED.name

    def get_status(self) -> dict:
        return {
            'parent_status': self.parent_status,
            'status': self.status
        }

    @classmethod
    def create_new_task_for_flow(cls, flow_id: str, task_node: TaskNode) -> object:
        now = datetime.utcnow()
        parent_tasks = [
            TaskAttribute(task_name=node.name, run_id="", status=TaskStatus.NOT_AVAILABLE.name) for node in task_node.parents
        ]
        child_tasks = [
            TaskAttribute(task_name=node.name, run_id="", status=TaskStatus.NOT_AVAILABLE.name) for node in task_node.children
        ]
        new_task = cls(
            task_name=task_node.name,
            run_id=str(uuid4()),
            flow_id=flow_id,
            created=now,
            status=TaskStatus.CREATED.name,
            parent_tasks=parent_tasks,
            child_tasks=child_tasks
        )
        new_task.save()
        return new_task

    def update_run_ids(self, task_node: TaskNode):
        for parent in self.parent_tasks:
            node = task_node.get_parent(parent.task_name)
            parent.run_id = node.run_id

        for child in self.child_tasks:
            node = task_node.get_child(child.task_name)
            child.run_id = node.run_id

        self.update(actions=[
            TaskModel.parent_tasks.set(self.parent_tasks),
            TaskModel.child_tasks.set(self.child_tasks)
        ])

    @classmethod
    def get_from_sfn_input(cls, event: dict) -> object:
        return cls.get(event["task_name"], event["run_id"])

    def update_task_status(self, status: TaskStatus) -> None:
        self.update(actions=[
            TaskModel.status.set(status.name)
        ])
        return status.name

    def update_parent_task_status(self, parent: object) -> None:
        for task in self.parent_tasks:
            if task.task_name == parent.task_name and task.run_id == parent.run_id:
                task.status = parent.status
        self.update(actions=[
            TaskModel.parent_tasks.set(self.parent_tasks)
        ])

    def update_child_task_status(self, child: object) -> None:
        for task in self.child_tasks:
            if task.task_name == child.task_name and task.run_id == child.run_id:
                task.status = child.status
        self.update(actions=[
            TaskModel.child_tasks.set(self.child_tasks)
        ])
