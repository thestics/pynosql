import json
import os
import uuid
from pathlib import Path

from pysql.conf import DEFAULT_STORAGE_DIR
from pysql.storagemanager.data_index import Indexes
from pysql.storagemanager.delete_index import DeletionIndexInner
from pysql.util import read_lines

ID_FIELD_NAME = '_id'
CHAR_NUM_FIELD_NAME = '_char_no'


class StorageManager:

    def __init__(self, storage_dir = DEFAULT_STORAGE_DIR):
        self._storage_dir = storage_dir
        self._storage_file = Path(storage_dir) / 'pynosql.data'
        self._delete_file = Path(storage_dir) / 'pynosql.delete.data'

        index_file = Path(storage_dir) / 'pynosql.index.data'
        self._index = Indexes(file_path=index_file)
        self._deleted_index = DeletionIndexInner(self._delete_file)

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

        deleted_objects_count = 0
        for deleted_objects_count, o in enumerate(objects):
            self._deleted_index.mark_deleted(o[CHAR_NUM_FIELD_NAME])
        self._deleted_index.save()
        return deleted_objects_count

    def vacuum(self):
        """
        Overwrite the storage file with all deletions applied, reset delete index

        :return:
        """
