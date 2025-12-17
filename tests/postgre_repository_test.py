"""
Tests for PostgreSQLRepository
"""

import pytest
from unittest.mock import patch, ANY
from uuid import UUID, uuid4
import json
import datetime
from dataclasses import dataclass, fields, field as dc_field
from typing import Union, List, Optional

from rococo.repositories.postgresql.postgresql_repository import PostgreSQLRepository
from rococo.models.versioned_model import VersionedModel
from rococo.data.postgresql import PostgreSQLAdapter
from rococo.messaging.base import MessageAdapter


@dataclass(kw_only=True)
class TestVersionedModel(VersionedModel):
    # Override base fields for consistent UUID object usage in tests
    entity_id: UUID = dc_field(default_factory=uuid4)
    version: UUID = dc_field(default_factory=lambda: UUID(int=0))
    previous_version: Union[UUID, None] = dc_field(default=None)
    changed_by_id: Union[UUID, str, None] = dc_field(
        default_factory=lambda: UUID(int=0))

    name: Optional[str] = None  # Example custom field
    related_item_id: Optional[UUID] = dc_field(
        default=None)  # For fetch_related tests
    related_items_ids: Optional[List[UUID]] = dc_field(
        default_factory=list)  # For fetch_related (many)

    # active and changed_on are inherited

    def _process_field_value(self, val, convert_uuids, convert_datetime_to_iso_string):
        if isinstance(val, UUID):
            return str(val) if convert_uuids else val
        elif isinstance(val, list) and all(isinstance(item, UUID) for item in val):
            return [str(item) if convert_uuids else item for item in val]
        elif isinstance(val, datetime.datetime):
            return val.isoformat() if convert_datetime_to_iso_string else val
        return val

    def as_dict(self, convert_datetime_to_iso_string=False, convert_uuids=True, export_properties=True):
        # This as_dict should be robust enough for different calls
        data_dict = {}
        for f_info in fields(self):
            val = getattr(self, f_info.name, None)

            # Handle specific field 'active' which is bool
            if f_info.name == 'active':
                data_dict[f_info.name] = val
                continue  # Skip further processing for 'active' if it's already bool

            if val is None:
                # Include None for fields that are explicitly None, helps in asserting later
                if hasattr(self, f_info.name):  # Only if attribute exists
                    data_dict[f_info.name] = None
                continue

            data_dict[f_info.name] = self._process_field_value(val, convert_uuids, convert_datetime_to_iso_string)

        # Ensure 'name' is included if it's an attribute
        if hasattr(self, 'name'):
            data_dict['name'] = self.name  # Will be None if self.name is None
        if hasattr(self, 'related_item_id'):
            data_dict['related_item_id'] = self.related_item_id
        if hasattr(self, 'related_items_ids'):
            data_dict['related_items_ids'] = self.related_items_ids

        # Return all fields, including those that are None, for consistent structure
        # The _process_data_before_save method will handle None values appropriately for the DB.
        return data_dict

    @staticmethod
    def _is_uuid_type(field_type):
        return field_type is UUID or \
            (hasattr(field_type, '__origin__') and field_type.__origin__ is Union and UUID in getattr(
                field_type, '__args__', []))

    @staticmethod
    def _is_datetime_type(field_type):
        return field_type is datetime.datetime or \
            (hasattr(field_type, '__origin__') and field_type.__origin__ is Union and datetime.datetime in getattr(
                field_type, '__args__', []))

    @staticmethod
    def _is_list_uuid_type(field_type):
        return (hasattr(field_type, '__origin__') and field_type.__origin__ is list and
                len(getattr(field_type, '__args__', [])) == 1 and
                getattr(field_type, '__args__', [None])[0] is UUID)

    @classmethod
    def _convert_value(cls, f_info, val):
        field_type_actual = f_info.type
        
        if cls._is_uuid_type(field_type_actual):
            return cls._handle_uuid_conversion(val)
        elif cls._is_list_uuid_type(field_type_actual):
            return cls._handle_list_uuid_conversion(val)
        elif cls._is_datetime_type(field_type_actual):
            return cls._handle_datetime_conversion(val)
        return val

    @staticmethod
    def _handle_uuid_conversion(val):
        if isinstance(val, str):
            try:
                return UUID(val)
            except ValueError:
                return None
        if isinstance(val, UUID) or val is None:
            return val
        return val

    @staticmethod
    def _handle_list_uuid_conversion(val):
        if isinstance(val, list):
            if all(isinstance(item, str) for item in val):
                return [UUID(item) for item in val]
            if all(isinstance(item, UUID) for item in val):
                return val
        return []

    @staticmethod
    def _handle_datetime_conversion(val):
        if isinstance(val, str):
            try:
                iso_val = val
                if iso_val.endswith('Z'):
                    iso_val = iso_val[:-1] + '+00:00'
                return datetime.datetime.fromisoformat(iso_val)
            except ValueError:
                return None
        if isinstance(val, datetime.datetime) or val is None:
            return val
        return val

    @classmethod
    def from_dict(cls, data: dict):
        if data is None:
            return None

        init_args = {}
        for f_info in fields(cls):
            if f_info.name in data:
                init_args[f_info.name] = cls._convert_value(f_info, data[f_info.name])
        return cls(**init_args)


@pytest.fixture
def mock_adapter(mocker):
    adapter = mocker.Mock(spec=PostgreSQLAdapter)
    adapter.__enter__ = mocker.Mock(return_value=adapter)
    adapter.__exit__ = mocker.Mock()
    # PostgreSQLAdapter.get_one/get_many return list of dicts or dict, parse_db_response handles it.
    # For testing, let's assume adapter methods return the direct data structure.
    adapter.get_one.return_value = {}
    adapter.get_many.return_value = []
    adapter.get_count.return_value = 0
    return adapter


@pytest.fixture
def mock_message_adapter(mocker):
    adapter = mocker.Mock(spec=MessageAdapter)
    adapter.send_message = mocker.Mock()
    return adapter


@pytest.fixture
def test_user_id():
    return UUID("12345678-1234-5678-1234-567812345678")


@pytest.fixture
def repository(mock_adapter, mock_message_adapter, test_user_id):
    # Ensure user_id is passed as UUID to the repository
    return PostgreSQLRepository(
        db_adapter=mock_adapter,
        model=TestVersionedModel,
        message_adapter=mock_message_adapter,
        queue_name="test_postgres_queue",
        user_id=test_user_id
    )


@pytest.fixture
def model_instance(test_user_id):
    instance = TestVersionedModel(
        entity_id=UUID("abcdef01-1234-5678-9abc-def012345678"),
        name="PG Test Item",
        changed_by_id=test_user_id,  # test_user_id is UUID
        changed_on=datetime.datetime.now(
            datetime.timezone.utc) - datetime.timedelta(days=1),
        version=UUID(int=0),
        previous_version=None,
        active=True,
        related_item_id=uuid4()  # Sample related ID
    )
    return instance


def test_adjust_conditions():
    """Tests the _adjust_conditions class method."""
    test_uuid1 = uuid4()
    test_uuid2 = uuid4()
    conditions = {
        "id": test_uuid1,
        "name": "test",
        "tags": [test_uuid1, test_uuid2],
        "empty_list": [],
        "string_list": ["a", "b"]
    }
    # Use copy to avoid modifying original
    adjusted = PostgreSQLRepository._adjust_conditions(conditions.copy())
    # Single UUIDs are not changed by this function
    assert adjusted["id"] == test_uuid1
    assert adjusted["name"] == "test"
    assert adjusted["tags"] == [str(test_uuid1), str(test_uuid2)]
    assert adjusted["empty_list"] == []
    assert adjusted["string_list"] == ["a", "b"]


def test_get_one_existing_record(repository, mock_adapter, model_instance):
    # Data as PostgreSQLAdapter would return it (strings for UUIDs, datetimes as Python datetimes or ISO strings)
    # PostgreSQL typically returns datetime objects from psycopg2
    db_data_from_adapter = {
        'entity_id': str(model_instance.entity_id),
        'version': str(model_instance.version),
        'active': model_instance.active,  # bool
        'name': model_instance.name,
        'changed_by_id': str(model_instance.changed_by_id),
        'changed_on': model_instance.changed_on,  # datetime object
        'previous_version': None,
        'related_item_id': str(model_instance.related_item_id)
    }
    mock_adapter.get_one.return_value = db_data_from_adapter

    # Query with UUID object, adjust_conditions will stringify it if it's in a list
    # For single UUID value, adjust_conditions doesn't change it.
    # PostgreSQLRepository.get_one will call adjust_conditions.
    query_conditions = {'entity_id': model_instance.entity_id}
    result = repository.get_one(query_conditions)

    assert isinstance(result, TestVersionedModel)
    assert result.entity_id == model_instance.entity_id
    assert result.name == model_instance.name

    # Check that adjust_conditions was implicitly called by get_one
    # The adapter should receive conditions processed by adjust_conditions.
    # In this case, 'entity_id' value is a single UUID, adjust_conditions doesn't modify it.
    mock_adapter.get_one.assert_called_once_with(
        repository.table_name,
        query_conditions  # adjust_conditions doesn't change single UUID values
    )
    mock_adapter.__enter__.assert_called_once()
    mock_adapter.__exit__.assert_called_once()


def test_get_one_with_uuid_list_in_conditions(repository, mock_adapter, model_instance):
    uuid_val = uuid4()
    query_conditions = {'some_ids': [uuid_val]}
    expected_conditions_for_adapter = {'some_ids': [str(uuid_val)]}

    # We are testing conditions adjustment
    mock_adapter.get_one.return_value = None

    repository.get_one(query_conditions)

    mock_adapter.get_one.assert_called_once_with(
        repository.table_name,
        expected_conditions_for_adapter
    )


def test_get_many_records(repository, mock_adapter, test_user_id):
    now = datetime.datetime.now(datetime.timezone.utc)
    instance1 = TestVersionedModel(entity_id=UUID(int=1), name="Item 1", version=uuid4(
    ), changed_by_id=test_user_id, changed_on=now, active=True)
    instance2 = TestVersionedModel(entity_id=UUID(int=2), name="Item 2", version=uuid4(
    ), changed_by_id=test_user_id, changed_on=now, active=True)

    instance1_db_data = {
        'entity_id': str(instance1.entity_id), 'name': instance1.name, 'active': True,
        'version': str(instance1.version), 'changed_by_id': str(instance1.changed_by_id),
        'changed_on': instance1.changed_on, 'previous_version': None
    }
    instance2_db_data = {
        'entity_id': str(instance2.entity_id), 'name': instance2.name, 'active': True,
        'version': str(instance2.version), 'changed_by_id': str(instance2.changed_by_id),
        'changed_on': instance2.changed_on, 'previous_version': None
    }
    mock_adapter.get_many.return_value = [instance1_db_data, instance2_db_data]

    query_conditions = {'active': True}
    result = repository.get_many(conditions=query_conditions, sort=[
                                 ('name', 'ASC')], limit=10, offset=0)

    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0].entity_id == instance1.entity_id
    assert result[1].name == "Item 2"

    mock_adapter.get_many.assert_called_once_with(
        repository.table_name,
        query_conditions,  # adjust_conditions doesn't change this specific query_conditions
        [('name', 'ASC')],
        10,
        0
    )


def test_get_count(repository, mock_adapter):
    query_params = {"name": "Test Category",
                    "active": True}  # active is already bool
    # adjust_conditions inside get_count will process query_params
    # after merging defaults and query
    expected_db_conditions = {'latest': True,
                              'active': True, "name": "Test Category"}

    mock_adapter.get_count.return_value = 42

    count = repository.get_count(index="some_index", query=query_params)

    assert count == 42
    mock_adapter.get_count.assert_called_once_with(
        repository.table_name,
        expected_db_conditions,  # This should be the merged and adjusted conditions
        options={'hint': "some_index"}
    )


@patch.object(TestVersionedModel, 'prepare_for_save')
@patch.object(TestVersionedModel, 'as_dict')
def test_save_new_instance(mock_model_as_dict, mock_model_prepare_for_save, repository, mock_adapter, model_instance, test_user_id):
    # PostgreSQLRepository._process_data_before_save calls:
    # 1. super()._process_data_before_save -> instance.prepare_for_save(), then instance.as_dict(convert_datetime_to_iso_string=True) [Result Dict A]
    # 2. instance.as_dict(convert_datetime_to_iso_string=False, convert_uuids=False) [Result Dict B]
    # 3. Formats Dict B for PostgreSQL.

    new_version_uuid = UUID(int=1)
    changed_on_time = datetime.datetime.now(datetime.timezone.utc)
    original_previous_version = model_instance.version

    def prepare_for_save_side_effect(changed_by_id):
        model_instance.version = new_version_uuid
        model_instance.changed_on = changed_on_time
        model_instance.previous_version = original_previous_version
        model_instance.changed_by_id = changed_by_id  # Should be UUID
    mock_model_prepare_for_save.side_effect = prepare_for_save_side_effect

    # Dict A (from BaseRepository part of _process_data_before_save)
    dict_a_from_as_dict = {'entity_id': str(model_instance.entity_id), 'version': str(
        new_version_uuid), 'changed_on': changed_on_time.isoformat()}
    # Dict B (from PostgreSQLRepository part of _process_data_before_save)
    dict_b_from_as_dict = {  # Contains UUID and datetime objects
        'entity_id': model_instance.entity_id, 'version': new_version_uuid,
        'active': model_instance.active, 'name': model_instance.name,
        'changed_by_id': test_user_id, 'changed_on': changed_on_time,
        'previous_version': original_previous_version,
        'related_item_id': model_instance.related_item_id
    }
    mock_model_as_dict.side_effect = [dict_a_from_as_dict, dict_b_from_as_dict]

    # This is what PostgreSQLRepository._process_data_before_save will ultimately pass to the adapter
    # based on formatting dict_b_from_as_dict
    expected_data_for_adapter = {
        'entity_id': str(model_instance.entity_id).replace('-', ''),
        'version': str(new_version_uuid).replace('-', ''),
        'active': model_instance.active,  # psycopg2 handles bools
        'name': model_instance.name,
        'changed_by_id': str(test_user_id).replace('-', ''),
        'changed_on': changed_on_time.strftime('%Y-%m-%d %H:%M:%S'),
        'previous_version': str(original_previous_version).replace('-', ''),
        'related_item_id': str(model_instance.related_item_id).replace('-', '')
    }

    mock_adapter.get_move_entity_to_audit_table_query.return_value = (
        "AUDIT_SQL_PG", (str(model_instance.entity_id),))
    mock_adapter.get_save_query.return_value = (
        "SAVE_SQL_PG", tuple(expected_data_for_adapter.values()))
    mock_adapter.run_transaction.return_value = True

    result = repository.save(model_instance, send_message=False)

    model_instance.prepare_for_save.assert_called_once_with(
        changed_by_id=test_user_id)

    assert mock_model_as_dict.call_count == 2
    mock_model_as_dict.assert_any_call(
        convert_datetime_to_iso_string=True, export_properties=False)
    mock_model_as_dict.assert_any_call(
        convert_datetime_to_iso_string=False, convert_uuids=False, export_properties=False)

    mock_adapter.get_save_query.assert_called_once_with(
        repository.table_name,
        expected_data_for_adapter
    )
    mock_adapter.run_transaction.assert_called_once()
    assert result is model_instance


@patch.object(TestVersionedModel, 'prepare_for_save')
@patch.object(TestVersionedModel, 'as_dict')
def test_delete_instance(mock_model_as_dict, mock_model_prepare_for_save, repository, mock_adapter, model_instance, test_user_id):
    deleted_version_uuid = UUID(int=2)
    deleted_changed_on_time = datetime.datetime.now(datetime.timezone.utc)
    version_before_delete_save = model_instance.version

    def prepare_for_save_side_effect(changed_by_id):
        model_instance.version = deleted_version_uuid
        model_instance.changed_on = deleted_changed_on_time
        model_instance.previous_version = version_before_delete_save
        model_instance.changed_by_id = changed_by_id
    mock_model_prepare_for_save.side_effect = prepare_for_save_side_effect

    dict_a_delete = {'entity_id': str(
        model_instance.entity_id), 'active': False}  # active is False
    dict_b_delete = {
        'entity_id': model_instance.entity_id, 'version': deleted_version_uuid,
        'active': False,  # Reflects change by repository.delete()
        'name': model_instance.name, 'changed_by_id': test_user_id,
        'changed_on': deleted_changed_on_time, 'previous_version': version_before_delete_save,
        'related_item_id': model_instance.related_item_id
    }
    mock_model_as_dict.side_effect = [dict_a_delete, dict_b_delete]

    expected_data_for_adapter_delete = {
        'entity_id': str(model_instance.entity_id).replace('-', ''),
        'version': str(deleted_version_uuid).replace('-', ''),
        'active': False,
        'name': model_instance.name,
        'changed_by_id': str(test_user_id).replace('-', ''),
        'changed_on': deleted_changed_on_time.strftime('%Y-%m-%d %H:%M:%S'),
        'previous_version': str(version_before_delete_save).replace('-', ''),
        'related_item_id': str(model_instance.related_item_id).replace('-', '')
    }

    mock_adapter.get_move_entity_to_audit_table_query.return_value = (
        "AUDIT_SQL_DEL_PG", ())
    mock_adapter.get_save_query.return_value = ("SAVE_SQL_DEL_PG", ())
    mock_adapter.run_transaction.return_value = True

    result = repository.delete(model_instance)

    assert result is model_instance
    assert model_instance.active is False

    model_instance.prepare_for_save.assert_called_once_with(
        changed_by_id=test_user_id)
    assert mock_model_as_dict.call_count == 2

    mock_adapter.get_save_query.assert_called_once_with(
        repository.table_name, expected_data_for_adapter_delete
    )


@patch.object(TestVersionedModel, 'prepare_for_save')
@patch.object(TestVersionedModel, 'as_dict')
def test_save_with_message(mock_model_as_dict, mock_model_prepare_for_save, repository, mock_adapter, mock_message_adapter, model_instance, test_user_id):
    # PostgreSQLRepository._process_data_before_save calls as_dict twice.
    # BaseRepository.save calls as_dict a third time for the message.

    saved_version_uuid = UUID(int=314)
    saved_changed_on_time = datetime.datetime.now(datetime.timezone.utc)
    original_previous_version_for_save = model_instance.version

    def prepare_for_save_side_effect(changed_by_id):
        model_instance.version = saved_version_uuid
        model_instance.changed_on = saved_changed_on_time
        model_instance.previous_version = original_previous_version_for_save
        model_instance.changed_by_id = changed_by_id
    mock_model_prepare_for_save.side_effect = prepare_for_save_side_effect

    # Call 1 (BaseRepo part of _process_data_before_save): as_dict(convert_datetime_to_iso_string=True)
    dict_call_1 = {
        'entity_id': str(model_instance.entity_id), 'version': str(saved_version_uuid),
        'changed_on': saved_changed_on_time.isoformat(), 'active': True, 'name': model_instance.name,
        'changed_by_id': str(test_user_id), 'previous_version': str(original_previous_version_for_save),
        'related_item_id': str(model_instance.related_item_id)
    }
    # Call 2 (PostgresRepo part of _process_data_before_save): as_dict(convert_datetime_to_iso_string=False, convert_uuids=False)
    dict_call_2 = {  # Contains UUID and datetime objects
        'entity_id': model_instance.entity_id, 'version': saved_version_uuid,
        'active': True, 'name': model_instance.name,
        'changed_by_id': test_user_id, 'changed_on': saved_changed_on_time,
        'previous_version': original_previous_version_for_save,
        'related_item_id': model_instance.related_item_id
    }
    # Call 3 (BaseRepo.save for message): as_dict(convert_datetime_to_iso_string=True)
    # This uses the state of model_instance *after* prepare_for_save has run.
    # Same args, reflects updated instance state
    dict_call_3_for_message = dict_call_1

    mock_model_as_dict.side_effect = [
        dict_call_1, dict_call_2, dict_call_3_for_message]

    expected_data_for_adapter_save = {
        'entity_id': str(model_instance.entity_id).replace('-', ''),
        'version': str(saved_version_uuid).replace('-', ''),
        'active': True, 'name': model_instance.name,
        'changed_by_id': str(test_user_id).replace('-', ''),
        'changed_on': saved_changed_on_time.strftime('%Y-%m-%d %H:%M:%S'),
        'previous_version': str(original_previous_version_for_save).replace('-', ''),
        'related_item_id': str(model_instance.related_item_id).replace('-', '')
    }

    mock_adapter.get_move_entity_to_audit_table_query.return_value = (
        "AUDIT_SQL_MSG_PG", ())
    mock_adapter.get_save_query.return_value = ("SAVE_SQL_MSG_PG", ())
    mock_adapter.run_transaction.return_value = True

    result = repository.save(model_instance, send_message=True)

    assert result is model_instance
    model_instance.prepare_for_save.assert_called_once_with(
        changed_by_id=test_user_id)

    assert mock_model_as_dict.call_count == 3
    calls = mock_model_as_dict.call_args_list
    # Order of these two can be tricky if super() is called first vs last in _process_data_before_save
    # Based on current PostgreSQLRepository: super() is first.
    calls[0].assert_called_with(
        convert_datetime_to_iso_string=True, export_properties=False)
    calls[1].assert_called_with(
        convert_datetime_to_iso_string=False, convert_uuids=False, export_properties=False)
    calls[2].assert_called_with(
        convert_datetime_to_iso_string=True)  # BaseRepo message call

    mock_adapter.get_save_query.assert_called_once_with(
        repository.table_name, expected_data_for_adapter_save)
    mock_adapter.run_transaction.assert_called_once()

    mock_message_adapter.send_message.assert_called_once_with(
        repository.queue_name,
        json.dumps(dict_call_3_for_message)
    )

# Basic test for fetch_related (can be expanded)


@patch.object(PostgreSQLRepository, 'fetch_related_entities_for_field')
def test_get_one_with_fetch_related(mock_fetch_related, repository, mock_adapter, model_instance):
    db_data_from_adapter = model_instance.as_dict(
        convert_uuids=True)  # Simpler dict for this
    # ensure datetime obj
    db_data_from_adapter['changed_on'] = model_instance.changed_on
    mock_adapter.get_one.return_value = db_data_from_adapter

    related_data_mock = TestVersionedModel(name="Related Thing")
    mock_fetch_related.return_value = related_data_mock

    result = repository.get_one(
        conditions={'entity_id': str(model_instance.entity_id)},
        fetch_related=['related_item_id']
    )

    assert result is not None
    mock_fetch_related.assert_called_once_with(
        ANY, 'related_item_id')  # ANY for the instance
    assert hasattr(result, 'related_item_id')
    assert result.related_item_id == related_data_mock


@patch.object(PostgreSQLRepository, 'fetch_related_entities_for_field')
def test_get_many_with_fetch_related(mock_fetch_related, repository, mock_adapter, model_instance):
    instance_data_db = model_instance.as_dict(convert_uuids=True)
    instance_data_db['changed_on'] = model_instance.changed_on
    mock_adapter.get_many.return_value = [instance_data_db]

    related_data_mock_list = [TestVersionedModel(name="Related Item 1")]
    # Simulate fetch_related_entities_for_field being called for each instance and field
    mock_fetch_related.return_value = related_data_mock_list

    results = repository.get_many(
        conditions={'active': True},
        # Assuming this is a field that holds list of IDs
        fetch_related=['related_items_ids']
    )

    assert len(results) == 1
    result_instance = results[0]
    mock_fetch_related.assert_called_once_with(
        result_instance, 'related_items_ids')
    assert hasattr(result_instance, 'related_items_ids')
    assert result_instance.related_items_ids == related_data_mock_list

def test_get_count_no_query_no_index(repository, mock_adapter):
    # Default conditions
    expected_db_conditions = {'latest': True, 'active': True}

    mock_adapter.get_count.return_value = 10

    # Call without args
    count = repository.get_count()

    assert count == 10
    mock_adapter.get_count.assert_called_once_with(
        repository.table_name,
        expected_db_conditions,
        options=None
    )


# High Priority Tests: fetch_related_entities_for_field()


def test_fetch_related_one_to_many_relationship(repository, mock_adapter, model_instance):
    """Test fetch_related with one_to_many relationship type"""
    # Setup: Create mock field metadata with one_to_many relationship
    related_id = uuid4()
    model_instance.related_item_id = related_id

    # Mock related model
    class RelatedModel(VersionedModel):
        pass

    # Mock the adapter to return multiple related records
    related_record_1 = {'entity_id': str(uuid4()), 'name': 'Related 1'}
    related_record_2 = {'entity_id': str(uuid4()), 'name': 'Related 2'}
    mock_adapter.get_many.return_value = [related_record_1, related_record_2]

    # Mock the metadata for the field
    with patch('rococo.repositories.postgresql.postgresql_repository.fields') as mock_fields_func:
        from dataclasses import Field
        mock_field = Field(default=None, default_factory=None, init=True, repr=True,
                          hash=None, compare=True, metadata={
                              'field_type': 'entity_id',
                              'relationship': {
                                  'model': RelatedModel,
                                  'relation_type': 'one_to_many'
                              }
                          }, kw_only=False)
        mock_field.name = 'related_item_id'
        mock_fields_func.return_value = [mock_field]

        # Call fetch_related_entities_for_field
        result = repository.fetch_related_entities_for_field(
            model_instance, 'related_item_id'
        )

    # Verify result is a list of related models
    assert isinstance(result, list)
    assert len(result) == 2

    # Verify adapter was called with correct conditions
    mock_adapter.get_many.assert_called_once()
    call_args = mock_adapter.get_many.call_args
    assert call_args[0][0] == 'related_model'  # Table name
    # Note: _adjust_conditions only converts UUID lists, not single UUIDs
    assert call_args[0][1] == {'entity_id': related_id}  # Single UUID not converted


def test_fetch_related_many_to_many_relationship(repository, mock_adapter, model_instance):
    """Test fetch_related with many_to_many relationship type"""
    # Setup: Create list of related IDs
    related_ids = [uuid4(), uuid4()]
    model_instance.related_items_ids = related_ids

    # Mock related model
    class RelatedItemModel(VersionedModel):
        pass

    # Mock adapter to return multiple records
    related_records = [
        {'entity_id': str(related_ids[0]), 'name': 'Item 1'},
        {'entity_id': str(related_ids[1]), 'name': 'Item 2'}
    ]
    mock_adapter.get_many.return_value = related_records

    # Mock field metadata
    with patch('rococo.repositories.postgresql.postgresql_repository.fields') as mock_fields_func:
        from dataclasses import Field
        mock_field = Field(default=None, default_factory=None, init=True, repr=True,
                          hash=None, compare=True, metadata={
                              'field_type': 'entity_id',
                              'relationship': {
                                  'model': RelatedItemModel,
                                  'relation_type': 'many_to_many'
                              }
                          }, kw_only=False)
        mock_field.name = 'related_items_ids'
        mock_fields_func.return_value = [mock_field]

        result = repository.fetch_related_entities_for_field(
            model_instance, 'related_items_ids'
        )

    # Verify result
    assert isinstance(result, list)
    assert len(result) == 2

    # Verify adapter called with adjusted conditions (UUIDs converted to strings)
    mock_adapter.get_many.assert_called_once()
    call_args = mock_adapter.get_many.call_args
    assert call_args[0][1] == {'entity_id': [str(related_ids[0]), str(related_ids[1])]}


def test_fetch_related_single_relationship(repository, mock_adapter, model_instance):
    """Test fetch_related with single (default) relationship type"""
    related_id = uuid4()
    model_instance.related_item_id = related_id

    class RelatedModel(VersionedModel):
        pass

    # Mock adapter to return single record
    related_record = {'entity_id': str(related_id), 'name': 'Single Related'}
    mock_adapter.get_one.return_value = related_record

    # Mock field metadata without relation_type (defaults to single)
    with patch('rococo.repositories.postgresql.postgresql_repository.fields') as mock_fields_func:
        from dataclasses import Field
        mock_field = Field(default=None, default_factory=None, init=True, repr=True,
                          hash=None, compare=True, metadata={
                              'field_type': 'entity_id',
                              'relationship': {
                                  'model': RelatedModel
                                  # No relation_type, should default to single
                              }
                          }, kw_only=False)
        mock_field.name = 'related_item_id'
        mock_fields_func.return_value = [mock_field]

        result = repository.fetch_related_entities_for_field(
            model_instance, 'related_item_id'
        )

    # Verify result is a single model instance (not a list)
    assert isinstance(result, RelatedModel)

    # Verify adapter.get_one was called
    mock_adapter.get_one.assert_called_once()


def test_fetch_related_none_value_returns_none(repository, mock_adapter, model_instance):
    """Test fetch_related returns None when field value is None"""
    # Set the related field to None
    model_instance.related_item_id = None

    result = repository.fetch_related_entities_for_field(
        model_instance, 'related_item_id'
    )

    # Should return None without calling adapter
    assert result is None
    mock_adapter.get_one.assert_not_called()
    mock_adapter.get_many.assert_not_called()


def test_fetch_related_empty_list_returns_none(repository, mock_adapter, model_instance):
    """Test fetch_related returns None when field value is empty list"""
    # Set the related field to empty list
    model_instance.related_items_ids = []

    result = repository.fetch_related_entities_for_field(
        model_instance, 'related_items_ids'
    )

    # Should return None without calling adapter
    assert result is None
    mock_adapter.get_one.assert_not_called()
    mock_adapter.get_many.assert_not_called()


def test_fetch_related_missing_field_metadata(repository, mock_adapter, model_instance):
    """Test fetch_related returns None when field has no metadata"""
    model_instance.related_item_id = uuid4()

    # Mock fields to return field without any metadata
    with patch('rococo.repositories.postgresql.postgresql_repository.fields') as mock_fields_func:
        from dataclasses import Field
        mock_field = Field(default=None, default_factory=None, init=True, repr=True,
                          hash=None, compare=True, metadata={},  # Empty metadata
                          kw_only=False)
        mock_field.name = 'related_item_id'
        mock_fields_func.return_value = [mock_field]

        result = repository.fetch_related_entities_for_field(
            model_instance, 'related_item_id'
        )

    # Should return None since no metadata exists
    assert result is None
    mock_adapter.get_one.assert_not_called()
    mock_adapter.get_many.assert_not_called()


def test_fetch_related_no_relationship_key(repository, mock_adapter, model_instance):
    """Test fetch_related returns None when metadata has no 'relationship' key"""
    model_instance.related_item_id = uuid4()

    # Mock fields with metadata but no 'relationship' key
    with patch('rococo.repositories.postgresql.postgresql_repository.fields') as mock_fields_func:
        from dataclasses import Field
        mock_field = Field(default=None, default_factory=None, init=True, repr=True,
                          hash=None, compare=True, metadata={
                              'field_type': 'entity_id'
                              # No 'relationship' key
                          }, kw_only=False)
        mock_field.name = 'related_item_id'
        mock_fields_func.return_value = [mock_field]

        result = repository.fetch_related_entities_for_field(
            model_instance, 'related_item_id'
        )

    # Should return None since no relationship metadata
    assert result is None
    mock_adapter.get_one.assert_not_called()
    mock_adapter.get_many.assert_not_called()


def test_fetch_related_none_records_from_get_many(repository, mock_adapter, model_instance):
    """Test fetch_related returns None when adapter.get_many returns None"""
    related_id = uuid4()
    model_instance.related_item_id = related_id

    class RelatedModel(VersionedModel):
        pass

    # Mock adapter to return None
    mock_adapter.get_many.return_value = None

    # Mock field metadata with one_to_many
    with patch('rococo.repositories.postgresql.postgresql_repository.fields') as mock_fields_func:
        from dataclasses import Field
        mock_field = Field(default=None, default_factory=None, init=True, repr=True,
                          hash=None, compare=True, metadata={
                              'field_type': 'entity_id',
                              'relationship': {
                                  'model': RelatedModel,
                                  'relation_type': 'one_to_many'
                              }
                          }, kw_only=False)
        mock_field.name = 'related_item_id'
        mock_fields_func.return_value = [mock_field]

        result = repository.fetch_related_entities_for_field(
            model_instance, 'related_item_id'
        )

    # Should return None when adapter returns None
    assert result is None
    mock_adapter.get_many.assert_called_once()


# High Priority Tests: _process_data_before_save()


def test_process_data_versioned_model_field(repository, test_user_id):
    """Test _process_data_before_save with VersionedModel as field value"""
    # Create a nested VersionedModel instance
    nested_model = TestVersionedModel(
        entity_id=UUID(int=999),
        name="Nested Model"
    )

    # Create main instance with VersionedModel as field value
    main_instance = TestVersionedModel(
        entity_id=UUID(int=1),
        name="Main Model",
        changed_by_id=test_user_id
    )

    # Mock as_dict to return nested model
    with patch.object(TestVersionedModel, 'as_dict') as mock_as_dict:
        mock_as_dict.return_value = {
            'entity_id': main_instance.entity_id,
            'name': main_instance.name,
            'related_item_id': nested_model,  # VersionedModel instance
            'active': True,
            'version': main_instance.version,
            'changed_by_id': test_user_id,
            'changed_on': datetime.datetime.now(datetime.timezone.utc),
            'previous_version': None
        }

        # Mock fields to include metadata
        with patch('rococo.repositories.postgresql.postgresql_repository.fields') as mock_fields_func:
            from dataclasses import Field
            related_field = Field(default=None, default_factory=None, init=True, repr=True,
                                hash=None, compare=True, metadata={'field_type': 'entity_id'},
                                kw_only=False)
            related_field.name = 'related_item_id'
            mock_fields_func.return_value = [related_field]

            result = repository._process_data_before_save(main_instance)

    # Verify VersionedModel's entity_id was extracted and hyphens removed
    assert 'related_item_id' in result
    assert result['related_item_id'] == str(nested_model.entity_id).replace('-', '')


def test_process_data_dict_with_entity_id(repository, test_user_id):
    """Test _process_data_before_save with dict containing entity_id"""
    entity_uuid = UUID(int=999)
    main_instance = TestVersionedModel(
        entity_id=UUID(int=1),
        name="Main Model",
        changed_by_id=test_user_id
    )

    # Mock as_dict to return dict with entity_id key
    with patch.object(TestVersionedModel, 'as_dict') as mock_as_dict:
        mock_as_dict.return_value = {
            'entity_id': main_instance.entity_id,
            'name': main_instance.name,
            'related_item_id': {'entity_id': entity_uuid, 'name': 'Related'},  # Dict with entity_id
            'active': True,
            'version': main_instance.version,
            'changed_by_id': test_user_id,
            'changed_on': datetime.datetime.now(datetime.timezone.utc),
            'previous_version': None
        }

        # Mock fields with metadata
        with patch('rococo.repositories.postgresql.postgresql_repository.fields') as mock_fields_func:
            from dataclasses import Field
            related_field = Field(default=None, default_factory=None, init=True, repr=True,
                                hash=None, compare=True, metadata={'field_type': 'entity_id'},
                                kw_only=False)
            related_field.name = 'related_item_id'
            mock_fields_func.return_value = [related_field]

            result = repository._process_data_before_save(main_instance)

    # Verify entity_id was extracted from dict and hyphens removed
    assert result['related_item_id'] == str(entity_uuid).replace('-', '')


def test_process_data_none_fields_skipped(repository, test_user_id):
    """Test _process_data_before_save skips None field values"""
    main_instance = TestVersionedModel(
        entity_id=UUID(int=1),
        name=None,  # None value
        changed_by_id=test_user_id
    )

    with patch.object(TestVersionedModel, 'as_dict') as mock_as_dict:
        mock_as_dict.return_value = {
            'entity_id': main_instance.entity_id,
            'name': None,  # None value should be skipped
            'related_item_id': None,  # None value
            'active': True,
            'version': main_instance.version,
            'changed_by_id': test_user_id,
            'changed_on': datetime.datetime.now(datetime.timezone.utc),
            'previous_version': None
        }

        with patch('rococo.repositories.postgresql.postgresql_repository.fields') as mock_fields_func:
            from dataclasses import Field
            name_field = Field(default=None, default_factory=None, init=True, repr=True,
                             hash=None, compare=True, metadata={},
                             kw_only=False)
            name_field.name = 'name'
            related_field = Field(default=None, default_factory=None, init=True, repr=True,
                                hash=None, compare=True, metadata={'field_type': 'entity_id'},
                                kw_only=False)
            related_field.name = 'related_item_id'
            mock_fields_func.return_value = [name_field, related_field]

            result = repository._process_data_before_save(main_instance)

    # Verify None values remain None (not processed)
    assert result['name'] is None
    assert result['related_item_id'] is None


def test_process_data_uuid_hyphen_removal(repository, test_user_id):
    """Test _process_data_before_save removes hyphens from UUIDs"""
    test_uuid = UUID("12345678-1234-5678-1234-567812345678")
    main_instance = TestVersionedModel(
        entity_id=test_uuid,
        changed_by_id=test_user_id
    )

    with patch.object(TestVersionedModel, 'as_dict') as mock_as_dict:
        mock_as_dict.return_value = {
            'entity_id': test_uuid,  # UUID object
            'version': UUID(int=0),
            'active': True,
            'changed_by_id': test_user_id,
            'changed_on': datetime.datetime.now(datetime.timezone.utc),
            'previous_version': None
        }

        with patch('rococo.repositories.postgresql.postgresql_repository.fields') as mock_fields_func:
            from dataclasses import Field
            entity_field = Field(default=None, default_factory=None, init=True, repr=True,
                               hash=None, compare=True, metadata={},
                               kw_only=False)
            entity_field.name = 'entity_id'
            version_field = Field(default=None, default_factory=None, init=True, repr=True,
                                hash=None, compare=True, metadata={},
                                kw_only=False)
            version_field.name = 'version'
            mock_fields_func.return_value = [entity_field, version_field]

            result = repository._process_data_before_save(main_instance)

    # Verify UUIDs have hyphens removed
    assert result['entity_id'] == str(test_uuid).replace('-', '')
    assert result['version'] == str(UUID(int=0)).replace('-', '')
    assert '-' not in result['entity_id']
    assert '-' not in result['version']


def test_process_data_datetime_formatting(repository, test_user_id):
    """Test _process_data_before_save formats datetime objects correctly"""
    test_datetime = datetime.datetime(2023, 5, 15, 14, 30, 45, tzinfo=datetime.timezone.utc)
    main_instance = TestVersionedModel(
        entity_id=UUID(int=1),
        changed_by_id=test_user_id,
        changed_on=test_datetime
    )

    with patch.object(TestVersionedModel, 'as_dict') as mock_as_dict:
        mock_as_dict.return_value = {
            'entity_id': main_instance.entity_id,
            'changed_on': test_datetime,  # datetime object
            'active': True,
            'version': UUID(int=0),
            'changed_by_id': test_user_id,
            'previous_version': None
        }

        with patch('rococo.repositories.postgresql.postgresql_repository.fields') as mock_fields_func:
            from dataclasses import Field
            changed_on_field = Field(default=None, default_factory=None, init=True, repr=True,
                                   hash=None, compare=True, metadata={},
                                   kw_only=False)
            changed_on_field.name = 'changed_on'
            mock_fields_func.return_value = [changed_on_field]

            result = repository._process_data_before_save(main_instance)

    # Verify datetime is formatted as '%Y-%m-%d %H:%M:%S'
    expected_format = '2023-05-15 14:30:45'
    assert result['changed_on'] == expected_format


def test_process_data_mixed_field_types(repository, test_user_id):
    """Test _process_data_before_save with multiple field types combined"""
    test_uuid = UUID("abcdef01-1234-5678-9abc-def012345678")
    test_datetime = datetime.datetime(2023, 6, 20, 10, 15, 30, tzinfo=datetime.timezone.utc)
    nested_model = TestVersionedModel(entity_id=UUID(int=888), name="Nested")

    main_instance = TestVersionedModel(
        entity_id=test_uuid,
        name="Mixed Test",
        changed_by_id=test_user_id,
        changed_on=test_datetime
    )

    with patch.object(TestVersionedModel, 'as_dict') as mock_as_dict:
        mock_as_dict.return_value = {
            'entity_id': test_uuid,  # UUID
            'name': "Mixed Test",  # String
            'related_item_id': nested_model,  # VersionedModel
            'changed_on': test_datetime,  # datetime
            'active': True,  # bool
            'version': UUID(int=5),  # UUID
            'changed_by_id': test_user_id,  # UUID
            'previous_version': None
        }

        with patch('rococo.repositories.postgresql.postgresql_repository.fields') as mock_fields_func:
            from dataclasses import Field

            entity_field = Field(default=None, default_factory=None, init=True, repr=True,
                               hash=None, compare=True, metadata={},
                               kw_only=False)
            entity_field.name = 'entity_id'

            related_field = Field(default=None, default_factory=None, init=True, repr=True,
                                hash=None, compare=True, metadata={'field_type': 'entity_id'},
                                kw_only=False)
            related_field.name = 'related_item_id'

            changed_on_field = Field(default=None, default_factory=None, init=True, repr=True,
                                   hash=None, compare=True, metadata={},
                                   kw_only=False)
            changed_on_field.name = 'changed_on'

            version_field = Field(default=None, default_factory=None, init=True, repr=True,
                                hash=None, compare=True, metadata={},
                                kw_only=False)
            version_field.name = 'version'

            changed_by_field = Field(default=None, default_factory=None, init=True, repr=True,
                                   hash=None, compare=True, metadata={},
                                   kw_only=False)
            changed_by_field.name = 'changed_by_id'

            mock_fields_func.return_value = [
                entity_field, related_field, changed_on_field, version_field, changed_by_field
            ]

            result = repository._process_data_before_save(main_instance)

    # Verify all transformations applied correctly
    assert result['entity_id'] == str(test_uuid).replace('-', '')
    assert result['related_item_id'] == str(nested_model.entity_id).replace('-', '')
    assert result['changed_on'] == '2023-06-20 10:15:30'
    assert result['version'] == str(UUID(int=5)).replace('-', '')
    assert result['changed_by_id'] == str(test_user_id).replace('-', '')
    assert result['name'] == "Mixed Test"  # String unchanged
    assert result['active'] is True  # bool unchanged


# High Priority Tests: _fetch_related_for_instances()


def test_fetch_related_for_instances_empty_fetch_list(repository, model_instance):
    """Test _fetch_related_for_instances with empty fetch_related list"""
    instances = [model_instance]

    # Mock fetch_related_entities_for_field to track if it's called
    with patch.object(repository, 'fetch_related_entities_for_field') as mock_fetch:
        # Call with empty list
        repository._fetch_related_for_instances(instances, [])

        # Verify fetch_related_entities_for_field was not called
        mock_fetch.assert_not_called()

    # Instance should remain unchanged
    assert instances[0] == model_instance


def test_fetch_related_for_instances_multiple_fields(repository, test_user_id):
    """Test _fetch_related_for_instances with multiple related fields"""
    # Create instances with multiple related fields
    instance1 = TestVersionedModel(
        entity_id=UUID(int=1),
        name="Instance 1",
        changed_by_id=test_user_id,
        related_item_id=UUID(int=100),
        related_items_ids=[UUID(int=200), UUID(int=201)]
    )

    # Mock related entities
    related_single = TestVersionedModel(entity_id=UUID(int=100), name="Single Related")
    related_multiple = [
        TestVersionedModel(entity_id=UUID(int=200), name="Multi 1"),
        TestVersionedModel(entity_id=UUID(int=201), name="Multi 2")
    ]

    with patch.object(repository, 'fetch_related_entities_for_field') as mock_fetch:
        # Configure mock to return different values for different fields
        def side_effect(instance, field_name):
            if field_name == 'related_item_id':
                return related_single
            elif field_name == 'related_items_ids':
                return related_multiple
            return None

        mock_fetch.side_effect = side_effect

        # Call with multiple fields
        repository._fetch_related_for_instances(
            [instance1],
            ['related_item_id', 'related_items_ids']
        )

        # Verify fetch was called for each field
        assert mock_fetch.call_count == 2
        mock_fetch.assert_any_call(instance1, 'related_item_id')
        mock_fetch.assert_any_call(instance1, 'related_items_ids')

    # Verify fields were updated
    assert instance1.related_item_id == related_single
    assert instance1.related_items_ids == related_multiple


def test_fetch_related_for_instances_missing_attr(repository, test_user_id):
    """Test _fetch_related_for_instances when instance is missing the attribute"""
    # Create a mock instance that doesn't have the related_item_id attribute
    from unittest.mock import MagicMock

    mock_instance = MagicMock(spec=['entity_id', 'name', 'changed_by_id'])
    mock_instance.entity_id = UUID(int=1)
    mock_instance.name = "Instance without attr"
    mock_instance.changed_by_id = test_user_id

    with patch.object(repository, 'fetch_related_entities_for_field') as mock_fetch:
        # Call with field that doesn't exist on mock instance
        repository._fetch_related_for_instances(
            [mock_instance],
            ['related_item_id']  # This field doesn't exist in spec
        )

        # Verify fetch_related_entities_for_field was not called
        # because hasattr check fails
        mock_fetch.assert_not_called()


def test_fetch_related_for_instances_multiple_items(repository, test_user_id):
    """Test _fetch_related_for_instances with multiple instances"""
    # Create multiple instances
    instance1 = TestVersionedModel(
        entity_id=UUID(int=1),
        name="Instance 1",
        changed_by_id=test_user_id,
        related_item_id=UUID(int=100)
    )

    instance2 = TestVersionedModel(
        entity_id=UUID(int=2),
        name="Instance 2",
        changed_by_id=test_user_id,
        related_item_id=UUID(int=200)
    )

    instance3 = TestVersionedModel(
        entity_id=UUID(int=3),
        name="Instance 3",
        changed_by_id=test_user_id,
        related_item_id=UUID(int=300)
    )

    # Mock related entities for each
    related_1 = TestVersionedModel(entity_id=UUID(int=100), name="Related 1")
    related_2 = TestVersionedModel(entity_id=UUID(int=200), name="Related 2")
    related_3 = TestVersionedModel(entity_id=UUID(int=300), name="Related 3")

    with patch.object(repository, 'fetch_related_entities_for_field') as mock_fetch:
        # Configure mock to return different values based on instance
        call_count = 0

        def side_effect(instance, field_name):
            if instance.entity_id == UUID(int=1):
                return related_1
            elif instance.entity_id == UUID(int=2):
                return related_2
            elif instance.entity_id == UUID(int=3):
                return related_3
            return None

        mock_fetch.side_effect = side_effect

        # Call with multiple instances
        repository._fetch_related_for_instances(
            [instance1, instance2, instance3],
            ['related_item_id']
        )

        # Verify fetch was called for each instance
        assert mock_fetch.call_count == 3
        mock_fetch.assert_any_call(instance1, 'related_item_id')
        mock_fetch.assert_any_call(instance2, 'related_item_id')
        mock_fetch.assert_any_call(instance3, 'related_item_id')

    # Verify each instance was updated with its related entity
    assert instance1.related_item_id == related_1
    assert instance2.related_item_id == related_2
    assert instance3.related_item_id == related_3


# Medium Priority Tests: get_many() edge cases


def test_get_many_empty_results(repository, mock_adapter):
    """Test get_many returns empty list when adapter returns empty list"""
    # Mock adapter to return empty list
    mock_adapter.get_many.return_value = []

    result = repository.get_many(conditions={'active': True})

    # Should return empty list
    assert isinstance(result, list)
    assert len(result) == 0

    # Verify adapter was called
    mock_adapter.get_many.assert_called_once()


def test_get_many_single_dict_to_list_conversion(repository, mock_adapter, test_user_id):
    """Test get_many converts single dict to list (line 137-138)"""
    # Mock adapter to return a single dict instead of a list
    single_record = {
        'entity_id': str(UUID(int=1)),
        'name': 'Single Item',
        'active': True,
        'version': str(UUID(int=0)),
        'changed_by_id': str(test_user_id),
        'changed_on': datetime.datetime.now(datetime.timezone.utc),
        'previous_version': None
    }
    mock_adapter.get_many.return_value = single_record  # dict, not list

    result = repository.get_many(conditions={'active': True})

    # Should convert dict to list and return list of instances
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], TestVersionedModel)
    assert result[0].name == 'Single Item'


def test_get_many_none_conditions(repository, mock_adapter, test_user_id):
    """Test get_many with None conditions"""
    # Mock adapter response
    record = {
        'entity_id': str(UUID(int=1)),
        'name': 'Item',
        'active': True,
        'version': str(UUID(int=0)),
        'changed_by_id': str(test_user_id),
        'changed_on': datetime.datetime.now(datetime.timezone.utc),
        'previous_version': None
    }
    mock_adapter.get_many.return_value = [record]

    # Call with None conditions
    result = repository.get_many(conditions=None)

    # Should work without errors
    assert isinstance(result, list)
    assert len(result) == 1

    # Verify adapter was called with None (not adjusted)
    call_args = mock_adapter.get_many.call_args
    assert call_args[0][1] is None  # conditions should be None


def test_get_many_none_sort_uses_default(repository, mock_adapter, test_user_id):
    """Test get_many with None sort parameter"""
    record = {
        'entity_id': str(UUID(int=1)),
        'name': 'Item',
        'active': True,
        'version': str(UUID(int=0)),
        'changed_by_id': str(test_user_id),
        'changed_on': datetime.datetime.now(datetime.timezone.utc),
        'previous_version': None
    }
    mock_adapter.get_many.return_value = [record]

    # Call with None sort (should use default)
    result = repository.get_many(conditions={'active': True}, sort=None)

    assert isinstance(result, list)
    assert len(result) == 1

    # Verify adapter was called with None sort
    call_args = mock_adapter.get_many.call_args
    assert call_args[0][2] is None  # sort parameter


def test_get_many_limit_zero(repository, mock_adapter):
    """Test get_many with limit=0 edge case"""
    # Mock adapter to return empty (since limit is 0)
    mock_adapter.get_many.return_value = []

    result = repository.get_many(conditions={'active': True}, limit=0)

    # Should return empty list
    assert isinstance(result, list)
    assert len(result) == 0

    # Verify adapter was called with limit=0
    call_args = mock_adapter.get_many.call_args
    assert call_args[0][3] == 0  # limit parameter


# Medium Priority Tests: error conditions


def test_get_one_adapter_raises_exception(repository, mock_adapter):
    """Test get_one when adapter raises an exception"""
    # Mock adapter.get_one to raise an exception when called
    def raise_exception(*args, **kwargs):
        raise Exception("Database connection failed")

    mock_adapter.get_one = raise_exception

    # Ensure __exit__ returns None to not suppress exceptions
    mock_adapter.__exit__.return_value = None

    # Should propagate the exception
    with pytest.raises(Exception) as exc_info:
        repository.get_one(conditions={'entity_id': UUID(int=1)})

    assert "Database connection failed" in str(exc_info.value)


def test_get_count_with_uuid_list_adjustment(repository, mock_adapter):
    """Test get_count properly adjusts UUID lists in query"""
    uuid1 = uuid4()
    uuid2 = uuid4()

    # Query with UUID list that needs adjustment
    query_with_uuids = {
        'tags': [uuid1, uuid2],  # List of UUIDs
        'active': True
    }

    mock_adapter.get_count.return_value = 5

    count = repository.get_count(query=query_with_uuids)

    assert count == 5

    # Verify adapter was called with adjusted conditions (UUIDs converted to strings)
    call_args = mock_adapter.get_count.call_args
    conditions = call_args[0][1]

    # The conditions should have UUIDs converted to strings
    assert conditions['tags'] == [str(uuid1), str(uuid2)]
    assert conditions['active'] is True
    assert conditions['latest'] is True  # Default condition added


def test_fetch_related_none_return_not_updated(repository, test_user_id):
    """Test that when fetch_related_entities_for_field returns None, field is not updated"""
    instance = TestVersionedModel(
        entity_id=UUID(int=1),
        name="Test",
        changed_by_id=test_user_id,
        related_item_id=UUID(int=100)  # Has a value
    )

    original_value = instance.related_item_id

    # Mock fetch_related_entities_for_field to return None
    with patch.object(repository, 'fetch_related_entities_for_field') as mock_fetch:
        mock_fetch.return_value = None  # Returns None

        repository._fetch_related_for_instances([instance], ['related_item_id'])

        # Verify fetch was called
        mock_fetch.assert_called_once_with(instance, 'related_item_id')

    # Field should NOT be updated when fetch returns None (line 86-87 in implementation)
    # The original value should remain
    assert instance.related_item_id == original_value


# Medium Priority Tests: __init__() table name conversion


def test_init_table_name_camelcase_to_snakecase(mock_adapter, mock_message_adapter, test_user_id):
    """Test __init__ converts CamelCase model name to snake_case table name"""
    # Create a model with CamelCase name
    @dataclass(kw_only=True)
    class MyTestModel(VersionedModel):
        name: Optional[str] = None

    # Create repository with CamelCase model
    repo = PostgreSQLRepository(
        db_adapter=mock_adapter,
        model=MyTestModel,
        message_adapter=mock_message_adapter,
        queue_name="test_queue",
        user_id=test_user_id
    )

    # Verify table name is converted to snake_case
    assert repo.table_name == "my_test_model"


def test_init_with_all_parameters(mock_adapter, mock_message_adapter, test_user_id):
    """Test __init__ sets all attributes correctly"""
    queue_name = "test_postgres_queue"

    repo = PostgreSQLRepository(
        db_adapter=mock_adapter,
        model=TestVersionedModel,
        message_adapter=mock_message_adapter,
        queue_name=queue_name,
        user_id=test_user_id
    )

    # Verify all attributes are set
    assert repo.adapter == mock_adapter
    assert repo.model == TestVersionedModel  # BaseRepository uses 'model' not 'model_class'
    assert repo.message_adapter == mock_message_adapter
    assert repo.queue_name == queue_name
    assert repo.user_id == test_user_id
    assert repo.table_name == "test_versioned_model"  # CamelCase -> snake_case
