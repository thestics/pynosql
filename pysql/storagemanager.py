import ast
import json
import os
import uuid
from ast import literal_eval
from collections import defaultdict
from pathlib import Path
import typing as tp

from pysql.conf import DEFAULT_STORAGE_DIR
from pysql.datastructures.rb_set import RBSet
from pysql.interfaces import Saveable, Serializable
from pysql.util import read_lines


ID_FIELD_NAME = '_id'
CHAR_NUM_FIELD_NAME = '_char_no'


# TODO: factor out serialization in a mixin


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

    def add(self, key, value):
        self._rb_set[key] = value

    def remove(self, key):
        self._rb_set.delete(key)


class Indexes(Saveable):
    _file_mode_load = 'r'
    _file_mode_save = 'w'

    def __init__(self, index_file: tp.Union[str, Path]):
        self._index_map = defaultdict(Index)
        self._path = index_file

        self.load()

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
            f.write(json.dumps(self._index_map, default=default))

    def index_record(self, data: dict, new_data_start: int):
        for field_name, field_value in data.items():
            self._index_record_field(field_name, field_value, new_data_start)
        self.save()

    def rebuild_index(self, data_generator: tp.Generator[dict, tp.Any, tp.Any]):
        raise NotImplementedError('TODO')

    def _index_record_field(self, field_name, field_value, row_idx):
        self._index_map[field_name].add(field_value, row_idx)


T = tp.TypeVar('T')


class SortedList(list, tp.Generic[T]):

    def __init__(self: tp.List[T], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sort()

    def insert_sorted(self, val: T):
        i = 0
        self.insert(i, val)

        # 6 -> [] = [6]
        # 6 -> [1] = [6, 1]
        # i = 0: [6, 1] -> [1, 6]

        # while there's items left and current and second items
        # are not sorted, swap them and move on to the next
        while i < len(self) - 1:
            if self[i] >= self[i + 1]:
                self[i], self[i + 1] = self[i + 1], self[i]
                i += 1
            else:
                return


class DeletionIndex(Saveable):

    def __init__(self, file_path: tp.Union[str, Path]):
        self._path = file_path
        self._data: tp.Optional[SortedList[int]] = None
        self._init_data()

    def _init_data(self):
        if not os.path.exists(self._path):
            Path(self._path).touch()
        with open(self._path) as f:
            data = f.read()
            if not data:
                self._data = SortedList()
            else:
                self._data = SortedList(literal_eval(data))

    def save(self):
        with open(self._path) as f:
            f.write(str(self._data))

    def load(self):
        self._init_data()

    def reset(self):
        self._data = SortedList()
        self.save()

    def mark_deleted(self, line_no: int):
        self._data.insert_sorted(line_no)


class StorageManager:

    def __init__(self, storage_dir = DEFAULT_STORAGE_DIR):
        self._storage_dir = storage_dir
        self._storage_file = Path(storage_dir) / 'pynosql.data'
        self._delete_file = Path(storage_dir) / 'pynosql.delete.data'

        index_file = Path(storage_dir) / 'pynosql.index.data'
        self._index = Indexes(index_file=index_file)
        self._deleted_index = DeletionIndex(self._delete_file)

        for f_name in (self._storage_file, index_file):
            if not os.path.exists(f_name):
                f_name.touch(exist_ok=True)

    def _update_index(self, obj: dict, new_data_start_idx: int):
        self._index.index_record(obj, new_data_start_idx)

    @property
    def storage_size(self):
        return os.stat(self._storage_file).st_size

    def get_next_write_index(self, data_size=-1):
        # TODO: this can be used as a hook for more efficient writing mechanisms
        #       e.g. someone might prefer block-writes pattern instead of append log.
        #       This function would serve as a hook to redefine this behaviour
        return self.storage_size

    def create_object(self, obj: dict):
        obj[ID_FIELD_NAME] = str(uuid.uuid4())

        new_data_start_idx = self.get_next_write_index()
        with open(self._storage_file, 'a') as f:
            f.write(json.dumps(obj) + '\n')
        self._update_index(obj, new_data_start_idx)

    def _get_objects(self, include_charno=False, **constraints):
        # TODO: use index for search
        res = []
        with open(self._storage_file, 'r') as f:
            for char_no, line in read_lines(f):
                obj: dict = json.loads(line)
                meets_constraint = all(obj.get(k, object()) == v for k, v in constraints.items())

                if include_charno:
                    obj[CHAR_NUM_FIELD_NAME] = char_no

                if meets_constraint:
                    res.append(obj)
        return res

    def get_objects(self, **constraints):
        return self._get_objects(include_charno=False, **constraints)

    def delete_objects(self, **constraints):
        objects = self._get_objects(include_charno=True, **constraints)

        for o in objects:
            self._deleted_index.mark_deleted(o[CHAR_NUM_FIELD_NAME])

    def vacuum(self):
        """
        Overwrite the storage file with all deletions applied, reset delete index

        :return:
        """


if __name__ == '__main__':
    s = StorageManager()
    s.create_object({'a': 1, 'c': 300})
    print(s.get_objects())

    # l = SortedList([1, 3, 2])
    # print(l)
    # l.insert_sorted(10)
    # print(l)
    # l.insert_sorted(4)
    # print(l)