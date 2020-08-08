import collections 


class AbstractNode(object):

    def __init__(self):
        self.__parents = collections.OrderedDict()
        self.__children = collections.OrderedDict()

    @property
    def name(self):
        raise NotImplementedError()

    @property
    def parents(self) -> [object]:
        return list(self.__parents.values())

    @property
    def children(self) -> [object]:
        return list(self.__children.values())

    def add_parent(self, node: object):
        self.__parents[node.name] = node

    def add_child(self, node: object):
        self.__children[node.name] = node

    def get_parent(self, node_name: str) -> object:
        return self.__parents[node_name]

    def get_child(self, node_name: str) -> object:
        return self.__children[node_name]

    @property
    def has_parent(self) -> bool:
        return len(self.__parents.keys()) > 0

    @property
    def has_children(self) -> bool:
        return len(self.__children.keys()) > 0

    def to_json(self) -> dict:
        return {
            "name": self.name,
            "parents": [parent.name for parent in self.parents],
            "children": [child.name for child in self.children]
        }
