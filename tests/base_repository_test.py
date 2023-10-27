from unittest.mock import Mock, patch

import pytest

from rococo.data.base import DbAdapter
from rococo.models.versioned_model import VersionedModel
from rococo.repositories.base_repository import BaseRepository


class TestVersionedModel(VersionedModel):
    @classmethod
    def from_dict(cls, data):
        return TestVersionedModel()

    def as_dict(self, convert_datetime_to_iso_string=False):
        return {}

class TestBaseRepository:

    @pytest.fixture
    def mock_adapter(self):
        return Mock(spec=DbAdapter)

    @pytest.fixture
    def repository(self, mock_adapter):
        return BaseRepository(mock_adapter, TestVersionedModel)

    def test_get_one_existing_record(self, repository, mock_adapter):
        # Mocking adapter's get_one to return a record
        mock_adapter.get_one.return_value = {'id': 1, 'name': 'Test'}
        
        result = repository.get_one({'id': 1})

        assert isinstance(result, TestVersionedModel)
        mock_adapter.get_one.assert_called_with('testversionedmodel', {'id': 1})

    def test_get_one_non_existing_record(self, repository, mock_adapter):
        # Mocking adapter's get_one to return None
        mock_adapter.get_one.return_value = None
        
        result = repository.get_one({'id': 2})

        assert result is None
        mock_adapter.get_one.assert_called_with('testversionedmodel', {'id': 2})

    def test_get_many_records(self, repository, mock_adapter):
        # Mocking adapter's get_many to return a list of records
        mock_adapter.get_many.return_value = [{'id': 1, 'name': 'Test1'}, {'id': 2, 'name': 'Test2'}]
        
        result = repository.get_many()

        assert isinstance(result, list)
        assert len(result) == 2
        assert isinstance(result[0], TestVersionedModel)
        assert isinstance(result[1], TestVersionedModel)
        mock_adapter.get_many.assert_called_with('testversionedmodel', None, None, 100)

    def test_save(self, repository, mock_adapter):
        # Mocking adapter's save to simulate save action
        mock_adapter.save.return_value = True

        model_instance = TestVersionedModel()
        result = repository.save(model_instance)

        assert result is True
        mock_adapter.save.assert_called_with('testversionedmodel', {})

    def test_delete(self, repository, mock_adapter):
        # Mocking adapter's delete to simulate delete action
        mock_adapter.delete.return_value = True

        conditions = {'id': 1}
        result = repository.delete(conditions)

        assert result is True
        mock_adapter.delete.assert_called_with('testversionedmodel', conditions)


if __name__ == '__main__':
    pytest.main()
