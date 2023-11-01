import json
import os
import uuid
from pathlib import Path
from threading import Lock
from typing import Iterable

from pysql.conf import DEFAULT_STORAGE_DIR
from pysql.storagemanager.cfg import CHAR_NUM_FIELD_NAME, ID_FIELD_NAME
from pysql.storagemanager.data_index import Indexes
from pysql.storagemanager.delete_index import DeletionIndex
from pysql.util import read_lines


class FileOps:

    def __init__(self, path):
        self._path = path

    def all_records(self, include_charno=False):
        with open(self._path, 'r') as f:
            for char_no, line in read_lines(f):
                obj = json.loads(line)
                if include_charno:
                    obj[CHAR_NUM_FIELD_NAME] = char_no
                yield obj

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
        self._deletion_lock = Lock()

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

    # todo: multiple creations of the same object?
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
            if lines is None:
                lines = set(cur_line_nos)
            else:
                lines.intersection_update(cur_line_nos)

        not_deleted_lines = [
            char_no for char_no in lines
            if not self._deleted_index.is_deleted(char_no)
        ]
        return self.storage_file_ops.records_by_charno(
            not_deleted_lines,
            include_charno=include_charno
        )

    def _get_objects(self, include_charno=False, **constraints):
        if not constraints:
            objects = self.storage_file_ops.all_records(include_charno=True)
            not_deleted_objects = [o for o in objects if not self._deleted_index.is_deleted(o[CHAR_NUM_FIELD_NAME])]
            if not include_charno:
                for o in not_deleted_objects:
                    o.pop(CHAR_NUM_FIELD_NAME)
            return not_deleted_objects
        return self._get_objects_indexed(include_charno=include_charno, **constraints)

    def get_objects(self, **constraints):
        return self._get_objects(include_charno=False, **constraints)

    def delete_objects(self, **constraints):
        objects = self._get_objects(include_charno=True, **constraints)

        deleted_objects_count = 0
        with self._deleted_index.atomic as delete:
            for _, o in enumerate(objects):
                delete.mark_deleted(o[CHAR_NUM_FIELD_NAME])
                deleted_objects_count += 1

        return deleted_objects_count

    def vacuum(self):
        """
        Overwrite the storage file with all deletions applied, reset delete index

        :return:
        """
        new_storage_file = Path(str(self._storage_file) + '.new')
        prev_to_be_deleted_idx = 0

        # do we need lock here? `os.replace` is atomic on os level
        # according to pydocs
        with self._deletion_lock, open(self._storage_file) as in_fp, open(new_storage_file, 'w') as out_fp:
            # deleted index is always sorted
            for to_be_deleted_idx in self._deleted_index:
                char_count = to_be_deleted_idx - prev_to_be_deleted_idx
                chunk = in_fp.read(char_count)
                out_fp.write(chunk)
                # skip line as it's the one we want to delete. Save its length
                prev_to_be_deleted_idx += len(in_fp.readline())

            # carry over any remaining data
            out_fp.write(in_fp.read())
            os.replace(new_storage_file, self._storage_file)
            self._deleted_index.reset()
            self._index.rebuild(
                data_generator=self.storage_file_ops.all_records(include_charno=True)
            )
