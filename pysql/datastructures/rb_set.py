import typing as tp

from pysql.datastructures.rbtree import RedBlackTree, Node


class IntSet:

    def __init__(self, data: tp.Iterable[tp.Tuple[int, tp.Any]]):
        self._tree = RedBlackTree()

        for k, v in data:
            self._tree.insert(k, v)

    def __contains__(self, key: int):
        return not self._tree.search(key).is_null()

    def __str__(self):
        return f"IntSet({list(self._tree)})"

    def __repr__(self):
        return str(self)

    def dump(self) -> tp.List[tp.Tuple[int, tp.Any]]:
        """Encodes a tree to a single list."""
        if not self._tree.root:
            return []

        queue = [self._tree.root]
        result = []

        while queue:
            node = queue.pop(0)
            if node:
                result.append((node.get_key(), node.value))
                queue.append(node.left)
                queue.append(node.right)
            else:
                result.append(None)

        return result

    @classmethod
    def load(cls, data) -> tp.Optional["IntSet"]:
        if data is None or len(data) == 0:
            return None

        root = Node(data[0][0], data[0][1])
        queue = [root]
        i = 1
        size = 0

        while queue and i < len(data):
            node = queue.pop(0)

            if data[i] is not None:
                left_node = Node(data[i][0], data[i][1])
                node.left = left_node
                queue.append(left_node)
                size += 1
            i += 1

            if i < len(data) and data[i] is not None:
                right_node = Node(data[i][0], data[i][1])
                node.right = right_node
                queue.append(right_node)
                size += 1
            i += 1

        obj = cls([])
        tree = RedBlackTree()
        tree.root = root
        tree.size = size
        obj._tree = tree
        return obj
