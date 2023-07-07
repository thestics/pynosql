import json
import os
import uuid
from pathlib import Path

from pysql.conf import DEFAULT_STORAGE_DIR
from pysql.util import read_lines


class Storage:

    def __init__(self, storage_dir = DEFAULT_STORAGE_DIR):
        self._storage_dir = storage_dir
        self._storage_file = Path(storage_dir) / 'pynosql.data'
        self._index_file = Path(storage_dir) / 'pynosql.index.data'

        for fname in (self._storage_file, self._index_file):
            if not os.path.exists(fname):
                fname.touch(exist_ok=True)

    def _update_index(self, obj: dict):
        pass

    def create_object(self, obj: dict):
        obj['_id'] = str(uuid.uuid4())
        with open(self._storage_file, 'a') as f:
            f.write(json.dumps(obj) + '\n')
        self._update_index(obj)

    def get_objects(self, **constraints):
        res = []
        with open(self._storage_file, 'r') as f:
            for line in f.readlines():
                obj = json.loads(line)
                meets_constraint = all(obj.get(k, object()) == v for k, v in constraints.items())
                if meets_constraint:
                    res.append(obj)
        return res


if __name__ == '__main__':
    s = Storage()
    # s.create_object({'a': 1, 'b': 200})
    print(s.get_objects(c=1))