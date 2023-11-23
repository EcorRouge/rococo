"""
Tests for BaseRepository
"""

from unittest.mock import Mock

import pytest

from rococo.data.base import DbAdapter
from rococo.models.versioned_model import VersionedModel
from rococo.repositories.base_repository import BaseRepository
from rococo.messaging.base import MessageAdapter


class TestVersionedModel(VersionedModel):
    """
    Test Class for VersionedModel
    """
    @classmethod
    def from_dict(cls, data):
        return TestVersionedModel()

    def as_dict(self, convert_datetime_to_iso_string=False):
        return {}


class TestBaseRepository:
    """
    Test class for BaseRepository
    """
    @pytest.fixture
    def mock_adapter(self):
        """
        Mock for DbAdapter
        """
        adapter = Mock(spec=DbAdapter)
        adapter.__enter__ = Mock(return_value=adapter)
        adapter.__exit__ = Mock()
        return adapter

    @pytest.fixture
    def _mock_message_adapter(self):
        """
        Mock for MessageAdapter
        """
        adapter = Mock(spec=MessageAdapter)
        adapter.__enter__ = Mock(return_value=adapter)
        adapter.__exit__ = Mock()
        return adapter

    @pytest.fixture
    def repository(self, mock_adapter, _mock_message_adapter):
        """
        Fixture for BaseRepository
        """
        return BaseRepository(
            mock_adapter,
            TestVersionedModel,
            _mock_message_adapter,
            "test_queue_name")

    def test_get_one_existing_record(self, repository, mock_adapter):
        """
        Tests getting one existing record from TestVersionedModel
        """
        mock_adapter.get_one.return_value = {'id': 1, 'name': 'Test'}

        result = repository.get_one({'id': 1})

        assert isinstance(result, TestVersionedModel)
        mock_adapter.get_one.assert_called_with('testversionedmodel', {'id': 1}, fetch_related=None)
        mock_adapter.__enter__.assert_called()
        mock_adapter.__exit__.assert_called()

    def test_get_one_non_existing_record(self, repository, mock_adapter):
        """
        Tests getting one non existing record from TestVersionedModel
        """
        mock_adapter.get_one.return_value = None

        result = repository.get_one({'id': 2})

        assert result is None
        mock_adapter.get_one.assert_called_with('testversionedmodel', {'id': 2}, fetch_related=None)
        mock_adapter.__enter__.assert_called()
        mock_adapter.__exit__.assert_called()

    def test_get_many_records(self, repository, mock_adapter):
        """
        Test getting many records
        """
        mock_adapter.get_many.return_value = [
            {'id': 1, 'name': 'Test1'},
            {'id': 2, 'name': 'Test2'}
        ]

        result = repository.get_many()

        assert isinstance(result, list)
        assert len(result) == 2
        assert isinstance(result[0], TestVersionedModel)
        assert isinstance(result[1], TestVersionedModel)
        mock_adapter.get_many.assert_called_with('testversionedmodel', None, None, 100, fetch_related=None)
        mock_adapter.__enter__.assert_called()
        mock_adapter.__exit__.assert_called()

    def test_save(self, repository, mock_adapter):
        """
        Test saving a model instance
        """
        mock_adapter.save.return_value = True
        mock_adapter.move_entity_to_audit_table.return_value = None

        model_instance = TestVersionedModel()
        result = repository.save(model_instance)

        assert result is model_instance
        assert result.entity_id is not None
        assert result.version is not None

        mock_adapter.save.assert_called_with('testversionedmodel', {})
        mock_adapter.__enter__.assert_called()
        mock_adapter.__exit__.assert_called()

    def test_delete(self, repository, mock_adapter):
        """
        Test deleting by id
        """
        mock_adapter.save.return_value = True
        mock_adapter.move_entity_to_audit_table.return_value = None

        model_instance = TestVersionedModel()
        result = repository.delete(model_instance)

        assert result is model_instance
        assert model_instance.active == False
        mock_adapter.save.assert_called_with('testversionedmodel', {})
        mock_adapter.__enter__.assert_called()
        mock_adapter.__exit__.assert_called()

    def test_save_with_message(self, repository, mock_adapter, _mock_message_adapter):
        """
        Test saving and sending message
        """
        mock_adapter.save.return_value = True
        mock_adapter.move_entity_to_audit_table.return_value = None

        model_instance = TestVersionedModel()
        result = repository.save(model_instance, True)

        assert result is model_instance
        mock_adapter.save.assert_called_with('testversionedmodel', {})
        mock_adapter.__enter__.assert_called()
        mock_adapter.__exit__.assert_called()

if __name__ == '__main__':
    pytest.main()
