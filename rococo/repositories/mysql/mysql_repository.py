"""MySqlDbRepository class"""

import re
from uuid import UUID
from datetime import datetime
from dataclasses import fields
from typing import Any, Dict, List, Type, Union

from rococo.data import MySqlAdapter
from rococo.messaging import MessageAdapter
from rococo.models import VersionedModel
from rococo.repositories import BaseRepository


class MySqlRepository(BaseRepository):
    """MySqlRepository class"""

    CAMEL_TO_SNAKE_PATTERN = r'(?<!^)(?=[A-Z])'

    def __init__(
            self,
            db_adapter: MySqlAdapter,
            model: Type[VersionedModel],
            message_adapter: MessageAdapter,
            queue_name: str,
            user_id: UUID = None
    ):
        super().__init__(db_adapter, model, message_adapter, queue_name, user_id=user_id)
        self.table_name = re.sub(
            self.CAMEL_TO_SNAKE_PATTERN, '_', model.__name__).lower()
        self.model()

    def _process_data_before_save(self, instance: VersionedModel):
        # Step 1: Call prepare_for_save on the instance.
        # This updates version, changed_on, previous_version, changed_by_id on the instance.
        instance.prepare_for_save(changed_by_id=self.user_id)
        # Step 2: Get the dictionary from the instance.
        # We need raw Python types (UUID objects, datetime objects) to then format them
        # specifically for MySQL. So, convert_uuids=False and convert_datetime_to_iso_string=False.
        raw_data_dict = instance.as_dict(
            convert_datetime_to_iso_string=False,
            # This means UUIDs will be UUID objects, datetimes will be datetime objects
            convert_uuids=False,
            export_properties=self.save_calculated_fields
        )
        # Step 3: Apply MySQL-specific formatting to create the final data dictionary for the adapter.
        # This logic should be comprehensive for all VersionedModel fields and any potential custom fields.
        formatted_data_for_adapter = {}
        for field_name, field_value in raw_data_dict.items():
            if field_value is None:
                # For MySQL, it's often better to explicitly pass NULL than omit the key
                # if the column is nullable. If the column is NOT NULL and has a default,
                # omitting might be fine. For consistency, let's include None as NULL.
                formatted_data_for_adapter[field_name] = None
                continue

            if isinstance(field_value, UUID):
                # MySQL typically stores UUIDs as 32-char hex strings without hyphens (BINARY(16) often stores the bytes)
                # or as CHAR(36) with hyphens. The existing MySqlRepository._process_data_before_save
                # did str(field_value).replace('-', '').
                # MySqlAdapter's _build_condition_string also uses str(value) for UUID.
                # Let's stick to the .hex.replace('-', '') for data to be saved if that's consistent,
                # or ensure the adapter handles UUID objects correctly if passed (it seems to convert to string).
                # The original MySqlRepository did: str(field_value).replace('-', '')
                formatted_data_for_adapter[field_name] = str(
                    field_value).replace('-', '')
            elif isinstance(field_value, datetime):
                formatted_data_for_adapter[field_name] = field_value.strftime(
                    '%Y-%m-%d %H:%M:%S')
            elif isinstance(field_value, bool):  # e.g., 'active' field
                formatted_data_for_adapter[field_name] = 1 if field_value else 0
            # Add handling for lists (e.g., list of UUIDs) if your models require it and
            # they need special formatting for MySQL (e.g., comma-separated string).
            # VersionedModel itself doesn't have list fields by default.
            else:
                formatted_data_for_adapter[field_name] = field_value

        return formatted_data_for_adapter

    def _handle_nested_str_field(self, data, field, field_model_class, field_table_name, field_value):
        if field.name == 'entity_id':
            data[field.name] = UUID(field_value).hex
        else:
            field_data = {'entity_id': field_value}
            for _field in fields(field_model_class):
                joined_key = f'joined_{field.name}_{field_table_name}_{_field.name}'
                if joined_key in data:
                    field_data[_field.name] = data[joined_key]
            for data_field, data_value in data.items():
                if data_field.startswith('joined_'):
                    field_data[data_field] = data_value

            data[field.name] = self._process_record_recursive(
                field_data, field_model_class)

    def _handle_entity_id_field(self, data, field, model):
        field_model_class = field.metadata.get(
            'relationship', {}).get('model') or model
        field_table_name = re.sub(
            self.CAMEL_TO_SNAKE_PATTERN, '_', field_model_class.__name__).lower()

        field_value = data[field.name]

        if isinstance(field_value, list):
            data[field.name] = [self._process_record_recursive(
                obj, field_model_class) for obj in field_value]
        elif isinstance(field_value, dict):
            data[field.name] = self._process_record_recursive(
                field_value, field_model_class)
        elif isinstance(field_value, str):
            self._handle_nested_str_field(data, field, field_model_class, field_table_name, field_value)
        elif isinstance(field_value, UUID):
            # Already a UUID object, no action needed
            pass
        else:
            raise NotImplementedError

    def _process_record_recursive(self, data: dict, model):
        """Helper method to process a record recursively."""
        model()
        is_partial = not all(
            _field.name in data for _field in fields(model))
        for field in fields(model):
            if data.get(field.name) is None:
                continue

            if field.metadata.get('field_type') == 'entity_id':
                self._handle_entity_id_field(data, field, model)
        record = model.from_dict(data)
        record._is_partial = is_partial
        return record

    def _process_data_from_db(self, data):
        """Method to convert data dictionary fetched from MySQL to a VersionedModel instance."""
        if data is None:
            return None
        elif isinstance(data, list):
            for record in data:
                self._process_record_recursive(record, self.model)
        elif isinstance(data, dict):
            self._process_record_recursive(data, self.model)
        else:
            raise NotImplementedError

    def _build_join_statements(self, join_fields: List[str], additional_fields: List[str]) -> List[str]:
        """Helper method to build join statements."""
        join_stmt_list = []
        if join_fields:
            joined_fields = {}
            for field_name in join_fields:
                if '.' in field_name:
                    parent_field, child_field = field_name.rsplit('.', 1)
                    if parent_field not in joined_fields:
                        raise ValueError(
                            f"Parent field {parent_field} needs to be joined before joining {child_field} field. "
                            f"Raised while joining {field_name} for model {self.model.__name__}.")
                    parent_model = joined_fields[parent_field]
                else:
                    parent_model = self.model
                    child_field = field_name
                
                parent_table_name = re.sub(
                    self.CAMEL_TO_SNAKE_PATTERN, '_', parent_model.__name__).lower()
                
                join_field = next((field for field in fields(
                    parent_model) if field.name == child_field), None)
                
                if join_field is None or join_field.metadata.get('field_type') != 'entity_id':
                    raise ValueError(
                        f"Invalid join field {child_field} specified for model {parent_model.__name__}.")
                
                join_model = join_field.metadata.get(
                    'relationship').get('model')
                join_table_name = re.sub(
                    self.CAMEL_TO_SNAKE_PATTERN, '_', join_model.__name__).lower()
                
                join_stmt_list.append(
                    f'INNER JOIN {join_table_name} ON {parent_table_name}.{child_field}={join_table_name}.entity_id '
                    f'AND {join_table_name}.active=true')
                
                join_field_list = [
                    f'{join_table_name}.{_field.name} AS joined_{child_field}_{join_table_name}_{_field.name}' for
                    _field in fields(join_model)]
                additional_fields.extend(join_field_list)
                joined_fields[field_name] = join_model
        return join_stmt_list

    def _format_single_condition_value(self, v):
        """Helper method to separate format logic"""
        if isinstance(v, VersionedModel):
            return str(v.entity_id).replace('-', '')
        elif isinstance(v, (str, UUID)):
            return str(v).replace('-', '')
        else:
            raise NotImplementedError

    def _format_condition_value(self, value):
        """Helper method to format condition values."""
        if isinstance(value, list):
            if len(value) == 0:
                raise NotImplementedError(
                    "Filtering an attribute with an empty list is not supported.")
            return [self._format_single_condition_value(v) for v in value]
        elif value is None:
            return None
        else:
            return self._format_single_condition_value(value)

    def _process_conditions(self, conditions: Dict[str, Any]):
        """Helper method to process query conditions."""
        if conditions:
            for condition_name, value in conditions.copy().items():
                condition_field = next((field for field in fields(
                    self.model) if field.name == condition_name), None)
                
                if condition_field and condition_field.metadata.get('field_type') == 'entity_id':
                    conditions[condition_name] = self._format_condition_value(value)

    def get_one(self, conditions: Dict[str, Any] = None, join_fields: List[str] = None,
                additional_fields: List[str] = None) -> Union[VersionedModel, None]:
        """get one"""

        if additional_fields is None:
            additional_fields = []

        join_stmt_list = self._build_join_statements(join_fields, additional_fields)
        self._process_conditions(conditions)

        data = self._execute_within_context(
            self.adapter.get_one, self.table_name, conditions, join_statements=join_stmt_list,
            additional_fields=additional_fields
        )

        # Calls __post_init__ of model to import related models and update fields.
        self.model()

        self._process_data_from_db(data)

        if not data:
            return None
        return self.model.from_dict(data)

    def get_many(
            self,
            conditions: Dict[str, Any] = None,
            join_fields: List[str] = None,
            additional_fields: List[str] = None,
            sort: List[tuple] = None,
            limit: int = None,
            offset: int = None
    ) -> List[VersionedModel]:
        """get many"""
        if additional_fields is None:
            additional_fields = []

        join_stmt_list = self._build_join_statements(join_fields, additional_fields)
        self._process_conditions(conditions)

        records = self._execute_within_context(
            self.adapter.get_many, self.table_name, conditions, sort, limit, offset, join_statements=join_stmt_list,
            additional_fields=additional_fields
        )

        # If the adapter returned a single dictionary, wrap it in a list
        if isinstance(records, dict):
            records = [records]

        # Calls __post_init__ of model to import related models and update fields.
        self.model()

        self._process_data_from_db(records)

        return [self.model.from_dict(record) for record in records]
