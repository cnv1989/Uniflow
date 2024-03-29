from ..decorators.task import Task
from .task_node import TaskNode
from .abstract_node import AbstractNode


class Unigraph(object):

    def __init__(self, tasks: [Task]) -> None:
        self.__tasks = tasks
        self.__init_nodes()
        self.__build__graph()

    def __init_nodes(self):
        self.__nodes = {}
        for task in self.__tasks:
            self.__add_node(TaskNode(task))

    def __add_node(self, node: AbstractNode) -> None:
        if node.name in self.__nodes:
            raise Exception(f"Already encountered node by name {node.name}")
        self.__nodes[node.name] = node

    @property
    def nodes(self):
        return self.__nodes.values()

    def to_json(self):
        return [node.to_json() for node in self.nodes]


    def __update_node_relationships(self):
        for key, node in self.__nodes.items():
            for parent_name in node.task.dependencies:
                parent = self.__nodes[parent_name]
                node.add_parent(parent)
                parent.add_child(node)

    def get_edge_nodes(self) -> [AbstractNode]:
        edge_nodes = []
        for key, node in self.__nodes.items():
            if not node.has_parent:
                edge_nodes.append(node)
        return edge_nodes

    def __check_for_cycle(self, node: AbstractNode, visitation_map: {str: bool}, visited_from: AbstractNode) -> None:
        if visitation_map[node.name] and visitation_map[visited_from.name] == node.name:
            raise Exception(f"Found cycle at node {node.name}")

        visitation_map[node.name] = visited_from.name

        for child in node.children:
            self.__check_for_cycle(child, visitation_map, node)

    def __check_for_cycles(self):
        for edge_node in self.get_edge_nodes():
            visitation_map = {}
            for key, node in self.__nodes.items():
                visitation_map[key] = None
            self.__check_for_cycle(edge_node, visitation_map, edge_node)

    def __dfs(self, node: AbstractNode, visitation_map: {str: bool}) -> None:
        visitation_map[node.name] = True
        for child in node.children:
            self.__dfs(child, visitation_map)

    def __check_for_islands(self):
        visitation_map = {}
        for key, node in self.__nodes.items():
            visitation_map[key] = False

        for edge_node in self.get_edge_nodes():
            self.__dfs(edge_node, visitation_map)

        for node_name, visited in visitation_map.items():
            if not visited:
                raise Exception(f"Found island at {node_name}")

    def __build__graph(self):
        self.__update_node_relationships()
        self.__check_for_cycles()
        self.__check_for_islands()
