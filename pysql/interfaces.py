from abc import ABC, abstractmethod
import typing as tp


Serialized = tp.Union[str, bytes]


class Saveable(tp.Protocol):

    def load(self): pass

    def save(self): pass


class Serializable(tp.Protocol):

    def serialize(self): pass

    def deserialize(self, data): pass
