import json
import logging
import typing as tp
from collections import defaultdict
from pathlib import Path
import os

from pysql.datastructures.rb_set import RBSet
from pysql.interfaces import Serializable, Saveable
from pysql.storagemanager import cfg

logger = logging.getLogger(__name__)


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

        self.init_file_if_not_exists()
        self.load()

    def __getitem__(self, item):
        return self._index_map[item]

    def init_file_if_not_exists(self):
        exists = os.path.exists(self._path)
        if not exists:
            dir_path, file_name = os.path.split(self._path)
            os.makedirs(dir_path, exist_ok=True)
            with open(self._path, 'w'):
                pass
            logger.info(f'Indexes path does not exist. Creating path: {self._path}')

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

    def reset(self):
        self._index_map = defaultdict(Index)
        self.save()

    def _index_record(self, data: dict, new_data_start: int, save: bool = True):
        for field_name, field_value in data.items():
            self._index_record_field(field_name, field_value, new_data_start)
        if save:
            self.save()

    def index_record(self, data: dict, new_data_start: int):
        self._index_record(data, new_data_start, save=True)

    def rebuild(self, data_generator: tp.Generator[dict, tp.Any, tp.Any]):
        logger.info('Reindexing data.')
        self.reset()

        for obj in data_generator:
            data_start = obj[cfg.CHAR_NUM_FIELD_NAME]
            self._index_record(obj, data_start, save=False)
        self.save()

    def _index_record_field(self, field_name, field_value, row_idx):
        self._index_map[field_name].add(field_value, row_idx)
