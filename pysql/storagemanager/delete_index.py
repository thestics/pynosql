import os
import typing as tp
from ast import literal_eval
from pathlib import Path

from pysql.datastructures.sorted_list import SortedList
from pysql.interfaces import Saveable


class DeletionIndexInner(Saveable):

    def __init__(self, file_path: tp.Union[str, Path]):
        self._path = file_path
        self._data: tp.Optional[SortedList[int]] = None
        self._buffer = SortedList()
        self._init_data()

    def _flush_buffer(self):
        for item in self._buffer:
            self._data.insert_sorted(item)
        self._buffer.clear()

    def _reset_buffer(self):
        self._buffer.clear()

    def _reset_data(self):
        self._data.clear()

    def _init_data(self):
        if not os.path.exists(self._path):
            Path(self._path).touch()

        with open(self._path) as f:
            data = f.read()
            if not data:
                self._data = SortedList()
            else:
                self._data = SortedList(literal_eval(data))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensure that updated deletion index is not saved"""
        if exc_val is None:
            self._flush_buffer()
            self.save()
            self._reset_buffer()
        else:
            self._flush_buffer()

    def save(self):
        with open(self._path, 'w') as f:
            f.write(str(self._data))

    def load(self):
        self._init_data()

    def reset(self):
        self._data = SortedList()
        self._reset_buffer()
        self.save()

    def is_deleted(self, key):
        return False

    def mark_deleted(self, line_no: int):
        # insert in buffer first to preserve atomicity
        self._buffer.insert_sorted(line_no)
