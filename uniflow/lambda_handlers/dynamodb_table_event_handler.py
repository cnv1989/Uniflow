import logging

logger = logging.getLogger(__name__)


class DynamodbTableEventHandler(object):

    def __init__(self, event: dict, context: dict) -> None:
        logger.info(f"Event: {event}")
        logger.info(f"Context: {context}")
        self.__event = event
        self.__context = context

    @property
    def records(self):
        return self.__event['Records']

    def execute(self) -> None:
        raise NotImplementedError
