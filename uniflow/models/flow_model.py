import os
from uuid import uuid4
from datetime import datetime
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, UTCDateTimeAttribute


class FlowModel(Model):

    class Meta:
        table_name = os.getenv("FLOW_TABLE")
        region = os.getenv("AWS_REGION")

    flow_id = UnicodeAttribute(hash_key=True, attr_name="FlowId")
    created = UTCDateTimeAttribute(range_key=True, attr_name="Created")
    last_modified = UTCDateTimeAttribute(attr_name="LastModified")

    @classmethod
    def create_new_flow(cls) -> object:
        now = datetime.utcnow()
        new_flow = cls(
            flow_id=str(uuid4()),
            created=now,
            last_modified=now
        )

        new_flow.save()
        return new_flow
