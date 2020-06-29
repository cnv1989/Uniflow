import importlib


def get_flow_class_from_flow(flow):
    module_name, class_name = flow.rsplit(".", 1)
    flow_module = importlib.import_module(module_name)
    return getattr(flow_module, class_name)
