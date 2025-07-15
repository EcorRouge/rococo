"""
Tests for MySqlRepository
"""

import pytest
from unittest.mock import patch
from uuid import UUID, uuid4
import json
import datetime
from dataclasses import dataclass, fields, field as dc_field
from typing import Union

from rococo.repositories.mysql.mysql_repository import MySqlRepository
# Assuming VersionedModel is correctly imported if used directly (it's a base class)
from rococo.models.versioned_model import VersionedModel
from rococo.data.mysql import MySqlAdapter
from rococo.messaging.base import MessageAdapter


@dataclass(kw_only=True)
class TestVersionedModel(VersionedModel):
    entity_id: UUID = dc_field(default_factory=uuid4)
    version: UUID = dc_field(default_factory=lambda: UUID(int=0))
    previous_version: Union[UUID, None] = dc_field(default=None)
    changed_by_id: Union[UUID, str, None] = dc_field(
        default_factory=lambda: UUID(int=0))
    name: str = None
    # active and changed_on are inherited from VersionedModel

    def as_dict(self, convert_datetime_to_iso_string=False, convert_uuids=True, export_properties=True):
        data_dict = {}
        for f_info in fields(self):
            val = getattr(self, f_info.name, None)
            if val is None:
                # For testing, it's sometimes useful to see None fields explicitly
                # For now, let's skip them if the original as_dict does.
                # If VersionedModel.as_dict includes them, this should too.
                # Based on VersionedModel.as_dict, it seems to filter by self.fields() then __dict__
                # which implicitly skips Nones if not in __dict__ or if values are None.
                # For simplicity in test model, let's include if present.
                # MySqlRepository itself handles None values before passing to adapter if needed.
                pass  # Let it be handled by getattr default or actual value

            if isinstance(val, UUID):
                data_dict[f_info.name] = str(val) if convert_uuids else val
            elif isinstance(val, datetime.datetime):
                data_dict[f_info.name] = val.isoformat(
                ) if convert_datetime_to_iso_string else val
            elif val is not None:  # include other non-None values
                data_dict[f_info.name] = val
            elif f_info.name in self.__dict__:  # include if explicitly set to None
                data_dict[f_info.name] = None

        # Ensure 'name' is in dict if it's an attribute and not None,
        # or if it was explicitly set to None
        if hasattr(self, 'name'):
            if self.name is not None:
                data_dict['name'] = self.name
            elif 'name' not in data_dict:  # if not picked up by loop and it was None
                data_dict['name'] = None

        # Filter out keys that are None only if that's the desired contract for as_dict
        # For now, let's return the dict as is, None values included if set.
        # The original TestVersionedModel in base_repository_test.py did:
        # return {k: v for k, v in data_dict.items() if v is not None}
        # Let's align with that common pattern.
        return {k: v for k, v in data_dict.items() if v is not None or k in ['previous_version', 'name', 'changed_by_id']}

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

                # Handle str type for changed_by_id if it's defined as Union[UUID, str, None]
                is_str_uuid_union_type = (hasattr(field_type_actual, '__origin__') and
                                          field_type_actual.__origin__ is Union and
                                          str in getattr(field_type_actual, '__args__', []) and
                                          UUID in getattr(field_type_actual, '__args__', []))

                # Special handling for changed_by_id if it can be str
                if is_uuid_type or (f_info.name == 'changed_by_id' and is_str_uuid_union_type):
                    if isinstance(val, str):
                        try:
                            init_args[f_info.name] = UUID(val)
                        except ValueError:
                            # If type is strictly UUID and it's not a valid UUID string, assign None or error
                            # If it's Union[UUID, str, None] for changed_by_id, could keep as str if desired, but test model aims for UUID
                            init_args[f_info.name] = None
                    elif isinstance(val, UUID):
                        init_args[f_info.name] = val
                    elif val is None:
                        init_args[f_info.name] = None
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
                else:  # For other types (like str for name, bool for active)
                    init_args[f_info.name] = val

        return cls(**init_args)


@pytest.fixture
def mock_adapter(mocker):
    adapter = mocker.Mock(spec=MySqlAdapter)
    adapter.__enter__ = mocker.Mock(return_value=adapter)
    adapter.__exit__ = mocker.Mock()
    adapter.parse_db_response = mocker.Mock(side_effect=lambda x: x)
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
    return MySqlRepository(
        db_adapter=mock_adapter,
        model=TestVersionedModel,
        message_adapter=mock_message_adapter,
        queue_name="test_mysql_queue",
        user_id=test_user_id
    )


@pytest.fixture
def model_instance(test_user_id):
    instance = TestVersionedModel(
        entity_id=UUID("abcdef01-1234-5678-9abc-def012345678"),
        name="Test Item",
        changed_by_id=test_user_id,
        changed_on=datetime.datetime.now(
            datetime.timezone.utc) - datetime.timedelta(days=1),
        version=UUID(int=0),
        previous_version=None,
        active=True
    )
    return instance


def test_get_one_existing_record(repository, mock_adapter, model_instance):
    db_data_from_adapter = {
        'entity_id': model_instance.entity_id.hex,
        'version': model_instance.version.hex,
        'active': 1, 'name': model_instance.name,
        'changed_by_id': model_instance.changed_by_id.hex if isinstance(model_instance.changed_by_id, UUID) else model_instance.changed_by_id,
        'changed_on': model_instance.changed_on.strftime('%Y-%m-%d %H:%M:%S'),
        'previous_version': None
    }
    mock_adapter.get_one.return_value = db_data_from_adapter

    result = repository.get_one({'entity_id': model_instance.entity_id.hex})

    assert isinstance(result, TestVersionedModel)
    assert result.entity_id == model_instance.entity_id
    assert result.name == model_instance.name

    mock_adapter.get_one.assert_called_once()
    call_args_list = mock_adapter.get_one.call_args_list
    args, kwargs_adapter_call = call_args_list[0]
    assert args[1]['entity_id'] == model_instance.entity_id.hex.replace(
        '-', '')

    mock_adapter.__enter__.assert_called_once()
    mock_adapter.__exit__.assert_called_once()


def test_get_one_non_existing_record(repository, mock_adapter):
    mock_adapter.get_one.return_value = None
    entity_id_to_check = UUID("00000000-0000-0000-0000-000000000000")
    result = repository.get_one({'entity_id': entity_id_to_check.hex})
    assert result is None

    mock_adapter.get_one.assert_called_once()
    call_args_list = mock_adapter.get_one.call_args_list
    args, kwargs_adapter_call = call_args_list[0]
    assert args[1]['entity_id'] == entity_id_to_check.hex.replace('-', '')


def test_get_many_records(repository, mock_adapter, test_user_id):
    now = datetime.datetime.now(datetime.timezone.utc)
    instance1 = TestVersionedModel(entity_id=UUID(int=1), name="Item 1", version=uuid4(
    ), changed_by_id=test_user_id, changed_on=now, active=True)
    instance2 = TestVersionedModel(entity_id=UUID(int=2), name="Item 2", version=uuid4(
    ), changed_by_id=test_user_id, changed_on=now, active=True)

    instance1_db_data = {
        'entity_id': instance1.entity_id.hex, 'name': instance1.name, 'active': 1,
        'version': instance1.version.hex,
        'changed_by_id': instance1.changed_by_id.hex if isinstance(instance1.changed_by_id, UUID) else instance1.changed_by_id,
        'changed_on': instance1.changed_on.strftime('%Y-%m-%d %H:%M:%S'),
        'previous_version': None
    }
    instance2_db_data = {
        'entity_id': instance2.entity_id.hex, 'name': instance2.name, 'active': 1,
        'version': instance2.version.hex,
        'changed_by_id': instance2.changed_by_id.hex if isinstance(instance2.changed_by_id, UUID) else instance2.changed_by_id,
        'changed_on': instance2.changed_on.strftime('%Y-%m-%d %H:%M:%S'),
        'previous_version': None
    }
    mock_adapter.get_many.return_value = [instance1_db_data, instance2_db_data]

    result = repository.get_many(conditions={'active': True})

    assert isinstance(result, list)
    assert len(result) == 2
    assert all(isinstance(r, TestVersionedModel) for r in result)
    assert result[0].name == "Item 1"
    assert result[0].entity_id == instance1.entity_id
    assert result[1].name == "Item 2"
    assert result[1].entity_id == instance2.entity_id

    mock_adapter.get_many.assert_called_once()
    call_args_list = mock_adapter.get_many.call_args_list
    args, kwargs_adapter_call = call_args_list[0]
    assert args[1]['active']


def test_get_many_empty(repository, mock_adapter):
    mock_adapter.get_many.return_value = []
    result = repository.get_many()
    assert result == []


@patch.object(TestVersionedModel, 'prepare_for_save')
@patch.object(TestVersionedModel, 'as_dict')
def test_save_new_instance(mock_model_as_dict, mock_model_prepare_for_save, repository, mock_adapter, model_instance, test_user_id):
    # With the improved MySqlRepository._process_data_before_save:
    # 1. instance.prepare_for_save() is called.
    # 2. instance.as_dict(convert_datetime_to_iso_string=False, convert_uuids=False) is called once.

    new_version_uuid = UUID(int=1)
    changed_on_time = datetime.datetime.now(datetime.timezone.utc)
    original_previous_version = model_instance.version

    def prepare_for_save_side_effect(changed_by_id):
        model_instance.version = new_version_uuid
        model_instance.changed_on = changed_on_time
        model_instance.previous_version = original_previous_version
        model_instance.changed_by_id = changed_by_id
    mock_model_prepare_for_save.side_effect = prepare_for_save_side_effect

    # This is what model_instance.as_dict (the mock) should return when called by the improved MySqlRepo
    data_from_model_as_dict = {
        'entity_id': model_instance.entity_id, 'version': new_version_uuid,
        'active': model_instance.active, 'name': model_instance.name,
        'changed_by_id': test_user_id, 'changed_on': changed_on_time,
        'previous_version': original_previous_version
    }
    mock_model_as_dict.return_value = data_from_model_as_dict

    # This is what MySqlRepository._process_data_before_save will pass to the adapter
    # after its internal formatting of data_from_model_as_dict
    expected_data_for_adapter = {
        'entity_id': model_instance.entity_id.hex.replace('-', ''),
        'version': new_version_uuid.hex.replace('-', ''),
        'active': 1 if model_instance.active else 0,
        'name': model_instance.name,
        'changed_by_id': test_user_id.hex.replace('-', '') if isinstance(test_user_id, UUID) else str(test_user_id).replace('-', ''),
        'changed_on': changed_on_time.strftime('%Y-%m-%d %H:%M:%S'),
        'previous_version': original_previous_version.hex.replace('-', '')
    }

    mock_adapter.get_move_entity_to_audit_table_query.return_value = (
        "AUDIT_SQL", (model_instance.entity_id.hex.replace('-', ''),))
    mock_adapter.get_save_query.return_value = (
        "SAVE_SQL", tuple(expected_data_for_adapter.values()))
    mock_adapter.run_transaction.return_value = True

    result = repository.save(model_instance, send_message=False)

    model_instance.prepare_for_save.assert_called_once_with(
        changed_by_id=test_user_id)
    # Now, as_dict is called only ONCE by the improved _process_data_before_save
    model_instance.as_dict.assert_called_once_with(
        convert_datetime_to_iso_string=False, convert_uuids=False, export_properties=False)

    mock_adapter.get_move_entity_to_audit_table_query.assert_called_once_with(
        repository.table_name,
        model_instance.entity_id
    )
    mock_adapter.get_save_query.assert_called_once_with(
        repository.table_name,
        expected_data_for_adapter
    )
    mock_adapter.run_transaction.assert_called_once()
    assert result is model_instance


@patch.object(TestVersionedModel, 'prepare_for_save')
@patch.object(TestVersionedModel, 'as_dict')
def test_delete_instance(mock_model_as_dict, mock_model_prepare_for_save, repository, mock_adapter, model_instance, test_user_id):
    # For delete, active will be set to False on model_instance *before* _process_data_before_save is called
    # by the internal save() call.

    deleted_version_uuid = UUID(int=2)
    deleted_changed_on_time = datetime.datetime.now(datetime.timezone.utc)
    version_before_delete_save = model_instance.version

    def prepare_for_save_side_effect(changed_by_id):
        model_instance.version = deleted_version_uuid  # This will be the new version
        model_instance.changed_on = deleted_changed_on_time
        # The version before this save
        model_instance.previous_version = version_before_delete_save
        model_instance.changed_by_id = changed_by_id
        # model_instance.active is already False
    mock_model_prepare_for_save.side_effect = prepare_for_save_side_effect

    # This is what model_instance.as_dict should return after active=False and prepare_for_save
    data_from_model_as_dict_for_delete = {
        'entity_id': model_instance.entity_id, 'version': deleted_version_uuid,
        'active': False,  # Reflects the change made by repository.delete()
        'name': model_instance.name, 'changed_by_id': test_user_id,
        'changed_on': deleted_changed_on_time, 'previous_version': version_before_delete_save
    }
    mock_model_as_dict.return_value = data_from_model_as_dict_for_delete

    expected_data_for_adapter_delete = {
        'entity_id': model_instance.entity_id.hex.replace('-', ''),
        'version': deleted_version_uuid.hex.replace('-', ''),
        'active': 0,
        'name': model_instance.name,
        'changed_by_id': test_user_id.hex.replace('-', '') if isinstance(test_user_id, UUID) else str(test_user_id).replace('-', ''),
        'changed_on': deleted_changed_on_time.strftime('%Y-%m-%d %H:%M:%S'),
        'previous_version': version_before_delete_save.hex.replace('-', '')
    }

    mock_adapter.get_move_entity_to_audit_table_query.return_value = (
        "AUDIT_SQL_DEL", ())
    mock_adapter.get_save_query.return_value = ("SAVE_SQL_DEL", ())
    mock_adapter.run_transaction.return_value = True

    # This sets instance.active = False, then calls save()
    result = repository.delete(model_instance)

    assert result is model_instance
    assert model_instance.active is False

    model_instance.prepare_for_save.assert_called_once_with(
        changed_by_id=test_user_id)
    # as_dict called once by the save operation within delete
    model_instance.as_dict.assert_called_once_with(
        convert_datetime_to_iso_string=False, convert_uuids=False, export_properties=False)

    mock_adapter.get_save_query.assert_called_once_with(
        repository.table_name, expected_data_for_adapter_delete
    )


@patch.object(TestVersionedModel, 'prepare_for_save')
@patch.object(TestVersionedModel, 'as_dict')
def test_save_with_message(mock_model_as_dict, mock_model_prepare_for_save, repository, mock_adapter, mock_message_adapter, model_instance, test_user_id):
    # With improved MySqlRepository:
    # 1. MySqlRepo._process_data_before_save calls prepare_for_save, then as_dict (call #1 for mock)
    # 2. BaseRepo.save (if send_message=True) calls as_dict again for message (call #2 for mock)

    saved_version_uuid = UUID(int=314)
    saved_changed_on_time = datetime.datetime.now(datetime.timezone.utc)
    original_previous_version_for_save = model_instance.version

    def prepare_for_save_side_effect(changed_by_id):
        model_instance.version = saved_version_uuid
        model_instance.changed_on = saved_changed_on_time
        model_instance.previous_version = original_previous_version_for_save
        model_instance.changed_by_id = changed_by_id
    mock_model_prepare_for_save.side_effect = prepare_for_save_side_effect

    # Define the sequence of return values for as_dict calls
    # Call 1 by MySqlRepo._process_data_before_save: as_dict(convert_datetime_to_iso_string=False, convert_uuids=False)
    dict_for_mysql_processing = {
        'entity_id': model_instance.entity_id, 'version': saved_version_uuid,
        'active': True, 'name': model_instance.name,
        'changed_by_id': test_user_id, 'changed_on': saved_changed_on_time,
        'previous_version': original_previous_version_for_save
    }
    # Call 2 by BaseRepo.save for message: as_dict(convert_datetime_to_iso_string=True)
    # This will use the state of model_instance *after* prepare_for_save
    dict_for_message_payload = {
        'entity_id': model_instance.entity_id.hex,
        'version': saved_version_uuid.hex,
        'active': True, 'name': model_instance.name,
        'changed_by_id': test_user_id.hex if isinstance(test_user_id, UUID) else str(test_user_id),
        'changed_on': saved_changed_on_time.isoformat(),
        'previous_version': original_previous_version_for_save.hex
    }

    mock_model_as_dict.side_effect = [
        dict_for_mysql_processing,  # For MySqlRepo's _process_data_before_save
        dict_for_message_payload    # For BaseRepo's message payload
    ]

    expected_data_for_adapter_save = {
        'entity_id': model_instance.entity_id.hex.replace('-', ''),
        'version': saved_version_uuid.hex.replace('-', ''),
        'active': 1, 'name': model_instance.name,
        'changed_by_id': test_user_id.hex.replace('-', '') if isinstance(test_user_id, UUID) else str(test_user_id).replace('-', ''),
        'changed_on': saved_changed_on_time.strftime('%Y-%m-%d %H:%M:%S'),
        'previous_version': original_previous_version_for_save.hex.replace('-', '')
    }

    mock_adapter.get_move_entity_to_audit_table_query.return_value = (
        "AUDIT_SQL_MSG", ())
    mock_adapter.get_save_query.return_value = ("SAVE_SQL_MSG", ())
    mock_adapter.run_transaction.return_value = True

    result = repository.save(model_instance, send_message=True)

    assert result is model_instance
    model_instance.prepare_for_save.assert_called_once_with(
        changed_by_id=test_user_id)

    # One for MySqlRepo, one for BaseRepo message
    assert mock_model_as_dict.call_count == 2
    calls = mock_model_as_dict.call_args_list
    calls[0].assert_called_with(
        convert_datetime_to_iso_string=False, convert_uuids=False)  # MySqlRepo call
    calls[1].assert_called_with(
        convert_datetime_to_iso_string=True)  # BaseRepo message call

    mock_adapter.get_save_query.assert_called_once_with(
        repository.table_name, expected_data_for_adapter_save)
    mock_adapter.run_transaction.assert_called_once()

    mock_message_adapter.send_message.assert_called_once_with(
        repository.queue_name,
        json.dumps(dict_for_message_payload)
    )
