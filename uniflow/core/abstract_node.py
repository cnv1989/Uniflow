class AbstractNode(object):

    def __init__(self):
        self.__parents = []
        self.__children = []

    @property
    def name(self):
        raise NotImplementedError()

    @property
    def parents(self) -> [object]:
        return self.__parents

    @property
    def children(self) -> [object]:
        return self.__children

    def add_parent(self, node: object):
        self.parents.append(node)

    def add_child(self, node: object):
        self.children.append(node)

    @property
    def has_parent(self) -> bool:
        return len(self.__parents) > 0

    @property
    def has_children(self) -> bool:
        return len(self.__children) > 0

    def to_json(self):
        return {
            "name": self.name,
            "parents": [parent.name for parent in self.parents],
            "children": [child.name for child in self.children]
        }
