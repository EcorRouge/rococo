from unittest.mock import Mock, call, patch

import pytest

from rococo.data.base import DbAdapter
from rococo.models.versioned_model import VersionedModel
from rococo.repositories.base_repository import BaseRepository
from rococo.messaging.rabbitmq import RabbitMqConnection

class TestVersionedModel(VersionedModel):
    @classmethod
    def from_dict(cls, data):
        return TestVersionedModel()

    def as_dict(self, convert_datetime_to_iso_string=False):
        return {}


class TestBaseRepository:

    @pytest.fixture
    def mock_adapter(self):
        adapter = Mock(spec=DbAdapter)
        adapter.__enter__ = Mock(return_value=adapter)
        adapter.__exit__ = Mock()
        return adapter
    

    @pytest.fixture
    def mock_rabbit_adapter(self):
        adapter = Mock(spec=RabbitMqConnection)
        adapter.__enter__ = Mock(return_value=adapter)
        adapter.__exit__ = Mock()
        return adapter


    @pytest.fixture
    def repository(self, mock_adapter, mock_rabbit_adapter):
        return BaseRepository(mock_adapter, TestVersionedModel, mock_rabbit_adapter, "test_queue_name")

    def test_get_one_existing_record(self, repository, mock_adapter):
        mock_adapter.get_one.return_value = {'id': 1, 'name': 'Test'}
        
        result = repository.get_one({'id': 1})

        assert isinstance(result, TestVersionedModel)
        mock_adapter.get_one.assert_called_with('testversionedmodel', {'id': 1})
        mock_adapter.__enter__.assert_called()
        mock_adapter.__exit__.assert_called()

    def test_get_one_non_existing_record(self, repository, mock_adapter):
        mock_adapter.get_one.return_value = None
        
        result = repository.get_one({'id': 2})

        assert result is None
        mock_adapter.get_one.assert_called_with('testversionedmodel', {'id': 2})
        mock_adapter.__enter__.assert_called()
        mock_adapter.__exit__.assert_called()

    def test_get_many_records(self, repository, mock_adapter):
        mock_adapter.get_many.return_value = [{'id': 1, 'name': 'Test1'}, {'id': 2, 'name': 'Test2'}]
        
        result = repository.get_many()

        assert isinstance(result, list)
        assert len(result) == 2
        assert isinstance(result[0], TestVersionedModel)
        assert isinstance(result[1], TestVersionedModel)
        mock_adapter.get_many.assert_called_with('testversionedmodel', None, None, 100)
        mock_adapter.__enter__.assert_called()
        mock_adapter.__exit__.assert_called()

    def test_save(self, repository, mock_adapter):
        mock_adapter.save.return_value = True

        model_instance = TestVersionedModel()
        result = repository.save(model_instance)

        assert result is True
        mock_adapter.save.assert_called_with('testversionedmodel', {})
        mock_adapter.__enter__.assert_called()
        mock_adapter.__exit__.assert_called()

    def test_delete(self, repository, mock_adapter):
        mock_adapter.delete.return_value = True

        conditions = {'id': 1}
        result = repository.delete(conditions)

        assert result is True
        mock_adapter.delete.assert_called_with('testversionedmodel', conditions)
        mock_adapter.__enter__.assert_called()
        mock_adapter.__exit__.assert_called()


    def test_save_with_message(self, repository, mock_adapter, mock_rabbit_adapter):
        mock_adapter.save.return_value = True

        model_instance = TestVersionedModel()
        result = repository.save(model_instance,True)

        assert result is True
        mock_adapter.save.assert_called_with('testversionedmodel', {})
        mock_adapter.__enter__.assert_called()
        mock_adapter.__exit__.assert_called()

if __name__ == '__main__':
    pytest.main()
