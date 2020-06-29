import boto3
import requests
import logging


logger = logging.getLogger(__name__)


class ApiClient(object):
    CFN = boto3.resource("cloudformation")

    def __init__(self, stack_name):
        self.__stack_name = stack_name
        self.__stack = self.CFN.Stack(stack_name)

        self.__init_endpoint()

    def __init_endpoint(self) -> None:
        self.__endpoint = None
        endpoint_prefix = f'{self.__stack_name}ApiEndpoint'
        for output in self.__stack.outputs:
            if output['OutputKey'].startswith(endpoint_prefix):
                self.__endpoint = output['OutputValue']

        if self.__endpoint is None:
            raise Exception("Cannot find rest api endpoint in the cfn stack!")

    def ping(self) -> None:
        response = requests.get(self.__endpoint)
        logger.info(response.text)

    @property
    def flow_endpoint(self):
        api_base = self.__endpoint.rstrip("/")
        return f"{api_base}/flow/start"

    def start_flow(self) -> None:
        response = requests.post(self.flow_endpoint)
        logger.info(response.text)

    def run_task(self, task_name) -> None:
        pass
