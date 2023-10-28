import json
import os
import uuid
from pathlib import Path
from typing import Iterable

from pysql.conf import DEFAULT_STORAGE_DIR
from pysql.storagemanager.data_index import Indexes
from pysql.storagemanager.delete_index import DeletionIndex
from pysql.util import read_lines


ID_FIELD_NAME = '_id'
CHAR_NUM_FIELD_NAME = '_char_no'


class FileOps:

    def __init__(self, path):
        self._path = path

    def all_records(self):
        with open(self._path, 'r') as f:
            for char_no, line in read_lines(f):
                yield json.loads(line)

    def records_by_charno(self, charno_list: Iterable[int], include_charno=False):
        with open(self._path, 'r') as f:
            for char_no in charno_list:
                f.seek(char_no)
                line = f.readline()
                obj = json.loads(line)

                if include_charno:
                    obj[CHAR_NUM_FIELD_NAME] = char_no

                yield obj


class StorageManager:

    def __init__(self, storage_dir = DEFAULT_STORAGE_DIR):
        self._storage_dir = storage_dir
        self._storage_file = Path(storage_dir) / 'pynosql.data'
        self._delete_file = Path(storage_dir) / 'pynosql.delete.data'

        index_file = Path(storage_dir) / 'pynosql.index.data'
        self._index = Indexes(file_path=index_file)
        self._deleted_index = DeletionIndex(self._delete_file)

        for f_name in (self._storage_file, index_file):
            if not os.path.exists(f_name):
                f_name.touch(exist_ok=True)

    @property
    def storage_file_ops(self):
        return FileOps(self._storage_file)

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

    def _get_objects_indexed(self, include_charno=False, **constraints):
        lines = None
        for constraint_key, constraint_val in constraints.items():
            cur_line_nos = self._index[constraint_key][constraint_val] or set()
            if cur_line_nos is None:
                continue
            if lines is None:
                lines = set(cur_line_nos)
            else:
                lines.intersection_update(cur_line_nos)
        return self.storage_file_ops.records_by_charno(lines, include_charno=include_charno)

    def _get_objects(self, include_charno=False, **constraints):
        if not constraints:
            return self.storage_file_ops.all_records()
        return self._get_objects_indexed(include_charno=include_charno, **constraints)

    def get_objects(self, **constraints):
        return self._get_objects(include_charno=False, **constraints)

    def delete_objects(self, **constraints):
        objects = self._get_objects(include_charno=True, **constraints)

        deleted_objects_count = 0
        with self._deleted_index.atomic as delete:
            for deleted_objects_count, o in enumerate(objects):
                delete.mark_deleted(o[CHAR_NUM_FIELD_NAME])

        return deleted_objects_count

    def vacuum(self):
        """
        Overwrite the storage file with all deletions applied, reset delete index

        :return:
        """
