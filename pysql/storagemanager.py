import ast
import json
import os
import uuid
from ast import literal_eval
from pathlib import Path
import typing as tp

from pysql.conf import DEFAULT_STORAGE_DIR
from pysql.datastructures.rb_set import RBSet
from pysql.interfaces import Serializable
from pysql.util import read_lines


class Index(Serializable):

    def __init__(self, index_path: tp.Union[str, Path]):
        self._path = index_path
        self._rb_set = RBSet()

    def load(self):
        with open(self._path) as f:
            data = f.read()
            source: tp.List = literal_eval(data)
            self._rb_set = RBSet.load(source)

    def save(self):
        with open(self._path, 'w') as f:
            data = self._rb_set.dump_str()
            f.write(data)

    def add(self, key, value, persist_immediately=True):
        self._rb_set[key] = value

        if persist_immediately:
            self.save()

    def remove(self, key):
        self._rb_set.delete(key)


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


class DeleteIndex(Serializable):

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

    _id_field_name = '_id'
    _char_no_field_name = '_char_no'

    def __init__(self, storage_dir = DEFAULT_STORAGE_DIR):
        self._storage_dir = storage_dir
        self._storage_file = Path(storage_dir) / 'pynosql.data'
        self._delete_file = Path(storage_dir) / 'pynosql.delete.data'

        index_file = Path(storage_dir) / 'pynosql.index.data'
        self._index = Index(index_path=index_file)
        self._deleted_index = DeleteIndex(self._delete_file)

        for f_name in (self._storage_file, index_file):
            if not os.path.exists(f_name):
                f_name.touch(exist_ok=True)

    def _update_index(self, obj: dict):
        pass

    def create_object(self, obj: dict):
        obj[self._id_field_name] = str(uuid.uuid4())

        with open(self._storage_file, 'a') as f:
            f.write(json.dumps(obj) + '\n')
        self._update_index(obj)

    def _get_objects(self, include_charno=False, **constraints):
        # TODO: use index for search
        res = []
        with open(self._storage_file, 'r') as f:
            for char_no, line in read_lines(f):
                obj: dict = json.loads(line)
                meets_constraint = all(obj.get(k, object()) == v for k, v in constraints.items())

                if include_charno:
                    obj[self._char_no_field_name] = char_no

                if meets_constraint:
                    res.append(obj)
        return res

    def get_objects(self, **constraints):
        return self._get_objects(include_charno=False, **constraints)

    def delete_objects(self, **constraints):
        objects = self._get_objects(include_charno=True, **constraints)

        for o in objects:
            self._deleted_index.mark_deleted(o[self._char_no_field_name])


if __name__ == '__main__':
    s = StorageManager()
    # # s.create_object({'a': 1, 'b': 200})
    print(s.get_objects())

    # l = SortedList([1, 3, 2])
    # print(l)
    # l.insert_sorted(10)
    # print(l)
    # l.insert_sorted(4)
    # print(l)