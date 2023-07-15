import typing as tp


Serialized = tp.Union[str, bytes]


class Saveable(tp.Protocol):

    def load(self): pass

    def save(self): pass


class Serializable(tp.Protocol):

    def serialize(self): pass

    def deserialize(self, data): pass


class Comparable(tp.Protocol):

    def __lt__(self, other): pass

    def __gt__(self, other): pass

    def __eq__(self, other): pass
