import logging
from flask_serverless import Flask
from ..models.flow_model import FlowModel

logger = logging.getLogger(__name__)

app = Flask(__name__)
app.url_map.strict_slashes = False


@app.route('/')
def index():
    return "Welcome to Uniflow!"


@app.route('/flow/start', methods=["POST"])
def start_flow():
    new_flow = FlowModel.create_new_flow()
    return f"Started flow {new_flow.flowId}"
