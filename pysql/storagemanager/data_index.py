import json
import typing as tp
from collections import defaultdict
from pathlib import Path

from pysql.datastructures.rb_set import RBSet
from pysql.interfaces import Serializable, Saveable


class Index(Serializable):

    def __init__(self, rb_set: RBSet = None):
        self._rb_set = rb_set or RBSet()

    @classmethod
    def deserialize(cls, data: tp.Dict[int, tp.List]):
        source = list(data.values())
        return cls(RBSet.load(source))

    def serialize(self) -> tp.Dict[int, tp.List]:
        # we can't represent an array of arrays in JSON just like that.
        # So instead, we need to find a way how to represent our object
        # in json. One way to do that is to represent array as an object
        # where array indexes are keys and values are corresponding
        # object values
        data = self._rb_set.dump()
        keys = range(len(data))
        return dict(zip(keys, data))

    def __getitem__(self, item):
        node = self._rb_set[item]
        return node.value

    def add(self, key, value):
        self._rb_set[key] = value

    def remove(self, key):
        self._rb_set.delete(key)


class Indexes(Saveable):
    _file_mode_load = 'r'
    _file_mode_save = 'w'

    def __init__(self, file_path: tp.Union[str, Path]):
        self._index_map = defaultdict(Index)
        self._path = file_path

        self.load()

    def __getitem__(self, item):
        return self._index_map[item]

    def load(self):
        with open(self._path, self._file_mode_load) as f:
            indexes_data = json.loads(f.read() or '{}')
            self._index_map = defaultdict(Index)

            for index_name, index_data in indexes_data.items():
                idx = Index.deserialize(index_data)
                self._index_map[index_name] = idx

    def save(self):

        def default(o: Index):
            if not isinstance(o, Index):
                raise TypeError()
            return o.serialize()

        with open(self._path, self._file_mode_save) as f:
            f.write(json.dumps(self._index_map, default=default, indent=2))

    def index_record(self, data: dict, new_data_start: int):
        for field_name, field_value in data.items():
            self._index_record_field(field_name, field_value, new_data_start)
        self.save()

    def rebuild_index(self, data_generator: tp.Generator[dict, tp.Any, tp.Any]):
        raise NotImplementedError('TODO')

    def _index_record_field(self, field_name, field_value, row_idx):
        self._index_map[field_name].add(field_value, row_idx)
