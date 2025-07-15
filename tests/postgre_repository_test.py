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

from rococo.repositories.postgresql.postgresql_repository import PostgreSQLRepository, adjust_conditions
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

    name: str = None  # Example custom field
    related_item_id: Optional[UUID] = dc_field(
        default=None)  # For fetch_related tests
    related_items_ids: Optional[List[UUID]] = dc_field(
        default_factory=list)  # For fetch_related (many)

    # active and changed_on are inherited

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

            if isinstance(val, UUID):
                data_dict[f_info.name] = str(val) if convert_uuids else val
            elif isinstance(val, list) and all(isinstance(item, UUID) for item in val):
                data_dict[f_info.name] = [
                    str(item) if convert_uuids else item for item in val]
            elif isinstance(val, datetime.datetime):
                data_dict[f_info.name] = val.isoformat(
                ) if convert_datetime_to_iso_string else val
            else:
                data_dict[f_info.name] = val

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

    @classmethod
    def from_dict(cls, data: dict):
        if data is None:
            return None

        init_args = {}
        for f_info in fields(cls):
            if f_info.name in data:
                val = data[f_info.name]
                field_type_actual = f_info.type

                is_uuid_type = field_type_actual is UUID or \
                    (hasattr(field_type_actual, '__origin__') and field_type_actual.__origin__ is Union and UUID in getattr(
                        field_type_actual, '__args__', []))

                is_datetime_type = field_type_actual is datetime.datetime or \
                    (hasattr(field_type_actual, '__origin__') and field_type_actual.__origin__ is Union and datetime.datetime in getattr(
                        field_type_actual, '__args__', []))

                is_list_uuid_type = (hasattr(field_type_actual, '__origin__') and field_type_actual.__origin__ is list and
                                     len(getattr(field_type_actual, '__args__', [])) == 1 and
                                     getattr(field_type_actual, '__args__', [None])[0] is UUID)

                if is_uuid_type:
                    if isinstance(val, str):
                        try:
                            init_args[f_info.name] = UUID(val)
                        except ValueError:
                            init_args[f_info.name] = None
                    elif isinstance(val, UUID):
                        init_args[f_info.name] = val
                    elif val is None:
                        init_args[f_info.name] = None
                elif is_list_uuid_type:
                    if isinstance(val, list) and all(isinstance(item, str) for item in val):
                        init_args[f_info.name] = [UUID(item) for item in val]
                    elif isinstance(val, list) and all(isinstance(item, UUID) for item in val):
                        init_args[f_info.name] = val
                    else:
                        # Default to empty list if conversion fails or type mismatch
                        init_args[f_info.name] = []
                elif is_datetime_type:
                    if isinstance(val, str):
                        try:
                            iso_val = val
                            if iso_val.endswith('Z'):
                                iso_val = iso_val[:-1] + '+00:00'
                            init_args[f_info.name] = datetime.datetime.fromisoformat(
                                iso_val)
                        except ValueError:
                            init_args[f_info.name] = None
                    elif isinstance(val, datetime.datetime):
                        init_args[f_info.name] = val
                    elif val is None:
                        init_args[f_info.name] = None
                else:
                    init_args[f_info.name] = val

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
    """Tests the adjust_conditions helper function."""
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
    adjusted = adjust_conditions(conditions.copy())
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
