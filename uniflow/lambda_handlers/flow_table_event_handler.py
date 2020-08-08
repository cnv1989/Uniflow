from .dynamodb_table_event_handler import DynamodbTableEventHandler
from .flow_record_handler import FlowRecordHander


class FlowTableEventHandler(DynamodbTableEventHandler):
    def execute(self) -> None:
        for record in self.records:
            FlowRecordHander(record).process()
