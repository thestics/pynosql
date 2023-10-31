import pytest

from pysql.storagemanager.storage import StorageManager

@pytest.fixture
def storage_manager_mock(tmp_path):
    mng = StorageManager(tmp_path)
    return mng


def test_storage_create_obj(storage_manager_mock):
    storage_manager_mock.create_object({'a': 1, 'b': 2})
    objects = list(storage_manager_mock.get_objects())
    assert all('_id' in obj for obj in objects)

    for obj in objects:
        obj.pop('_id')

    assert list(objects) == [{'a': 1, 'b': 2}]
