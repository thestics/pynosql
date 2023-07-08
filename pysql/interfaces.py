from abc import ABC, abstractmethod


class Serializable(ABC):

    @abstractmethod
    def load(self): pass

    @abstractmethod
    def save(self): pass
