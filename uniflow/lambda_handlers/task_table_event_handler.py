from .dynamodb_table_event_handler import DynamodbTableEventHandler
from .task_record_handler import TaskRecordHandler


class TaskTableEventHandler(DynamodbTableEventHandler):
    def execute(self):
        for record in self.records:
            TaskRecordHandler(record).process()
