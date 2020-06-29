import logging

logger = logging.getLogger(__name__)


class TaskTableEventHandler(object):

    def __init__(self, event: dict, context: dict) -> None:
        self.__event = event
        self.__context = context

    def execute(self):
        logger.info("Hello World!")
        return self.__event
