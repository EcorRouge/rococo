"""MySqlDbRepository class"""

from dataclasses import fields
from datetime import datetime
import json
import re
from typing import Any, Dict, List, Type, Union
from uuid import UUID

from rococo.data import MySqlAdapter
from rococo.messaging import MessageAdapter
from rococo.models import VersionedModel
from rococo.repositories import BaseRepository


class MySqlRepository(BaseRepository):
    """MySqlRepository class"""

    def __init__(
            self,
            db_adapter: MySqlAdapter,
            model: Type[VersionedModel],
            message_adapter: MessageAdapter,
            queue_name: str,
            user_id: UUID = None
    ):
        super().__init__(db_adapter, model, message_adapter, queue_name, user_id=user_id)
        self.table_name = re.sub(r'(?<!^)(?=[A-Z])', '_', model.__name__).lower()
        self.model()

    def _process_data_before_save(self, instance: VersionedModel):
        """Method to convert VersionedModel instance to a data dictionary that can be saved to MySQL"""
        super()._process_data_before_save(instance)
        data = instance.as_dict(convert_datetime_to_iso_string=False, convert_uuids=False)
        for field in fields(instance):
            if data.get(field.name) is None:
                continue

            field_value = data[field.name]

            if field.metadata.get('field_type') in ['entity_id', 'uuid']:
                if isinstance(field_value, VersionedModel):
                    field_value = str(field_value.entity_id).replace('-', '')
                elif isinstance(field_value, dict):
                    field_value = str(field_value.get('entity_id')).replace('-', '')
                elif isinstance(field_value, str):
                    field_value = field_value.replace('-', '')

            if isinstance(field_value, UUID):
                field_value = str(field_value).replace('-', '')
            if isinstance(field_value, datetime):
                field_value = field_value.strftime('%Y-%m-%d %H:%M:%S')

            data[field.name] = field_value
        return data

    def _process_data_from_db(self, data):
        """Method to convert data dictionary fetched from MySQL to a VersionedModel instance."""

        def _process_record(data: dict, model):
            model()
            is_partial = not all(_field.name in data for _field in fields(model))
            for field in fields(model):
                if data.get(field.name) is None:
                    continue

                if field.metadata.get('field_type') == 'entity_id':
                    field_model_class = field.metadata.get('relationship', {}).get('model') or model
                    field_table_name = re.sub(r'(?<!^)(?=[A-Z])', '_', field_model_class.__name__).lower()

                    field_value = data[field.name]

                    if isinstance(field_value, list):
                        data[field.name] = [_process_record(obj, field_model_class) for obj in field_value]
                    elif isinstance(field_value, dict):
                        data[field.name] = _process_record(field_value, field_model_class)
                    elif isinstance(field_value, str):
                        if field.name == 'entity_id':
                            data[field.name] = UUID(field_value)
                        else:
                            field_data = {'entity_id': field_value}
                            for _field in fields(field_model_class):
                                if f'joined_{field.name}_{field_table_name}_{_field.name}' in data:
                                    field_data[_field.name] = data[
                                        f'joined_{field.name}_{field_table_name}_{_field.name}']
                            for data_field, data_value in data.items():
                                if data_field.startswith('joined_'):
                                    field_data[data_field] = data_value

                            data[field.name] = _process_record(field_data, field_model_class)
                    elif isinstance(field_value, UUID):
                        pass
                    else:
                        raise NotImplementedError
            record = model.from_dict(data)
            record._is_partial = is_partial
            return record

        if data is None:
            return None
        elif isinstance(data, list):
            for record in data:
                _process_record(record, self.model)
        elif isinstance(data, dict):
            _process_record(data, self.model)
        else:
            raise NotImplementedError

    def get_one(self, conditions: Dict[str, Any] = None, join_fields: List[str] = None,
                additional_fields: List[str] = None) -> Union[VersionedModel, None]:
        """get one"""

        if additional_fields is None:
            additional_fields = []

        join_stmt_list = []
        if join_fields:
            joined_fields = {}
            for field_name in join_fields:
                if '.' in field_name:
                    parent_field, child_field = field_name.rsplit('.', 1)
                    if parent_field not in joined_fields:
                        raise Exception(
                            f"Parent field {parent_field} needs to be joined before joining {child_field} field. Raised while joining {field_name} for model {self.model.__name__}.")
                    parent_model = joined_fields[parent_field]
                else:
                    parent_model = self.model
                    child_field = field_name
                parent_table_name = re.sub(r'(?<!^)(?=[A-Z])', '_', parent_model.__name__).lower()
                join_field = next((field for field in fields(parent_model) if field.name == child_field), None)
                if join_field is None or join_field.metadata.get('field_type') != 'entity_id':
                    raise Exception(f"Invalid join field {child_field} specified for model {parent_model.__name__}.")
                join_model = join_field.metadata.get('relationship').get('model')
                join_table_name = re.sub(r'(?<!^)(?=[A-Z])', '_', join_model.__name__).lower()
                join_stmt_list.append(
                    f'INNER JOIN {join_table_name} ON {parent_table_name}.{child_field}={join_table_name}.entity_id AND {join_table_name}.active=true')
                join_field_list = [
                    f'{join_table_name}.{_field.name} AS joined_{child_field}_{join_table_name}_{_field.name}' for
                    _field in fields(join_model)]
                additional_fields += join_field_list
                joined_fields[field_name] = join_model

        if conditions:
            for condition_name, value in conditions.copy().items():
                condition_field = next((field for field in fields(self.model) if field.name == condition_name), None)
                if condition_field and condition_field.metadata.get('field_type') == 'entity_id':
                    if isinstance(value, VersionedModel):
                        conditions[condition_name] = str(value.entity_id).replace('-', '')
                    elif isinstance(value, (str, UUID)):
                        conditions[condition_name] = str(value).replace('-', '')
                    elif isinstance(value, list):
                        # Handle list
                        conditions[condition_name] = []
                        for v in value:
                            if isinstance(v, VersionedModel):
                                conditions[condition_name].append(str(v.entity_id).replace('-', ''))
                            elif isinstance(v, (str, UUID)):
                                conditions[condition_name].append(str(v).replace('-', ''))
                            else:
                                raise NotImplementedError
                    else:
                        raise NotImplementedError

        data = self._execute_within_context(
            self.adapter.get_one, self.table_name, conditions, join_statements=join_stmt_list,
            additional_fields=additional_fields
        )

        self.model()  # Calls __post_init__ of model to import related models and update fields.

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

        join_stmt_list = []
        if join_fields:
            joined_fields = {}
            for field_name in join_fields:
                if '.' in field_name:
                    parent_field, child_field = field_name.rsplit('.', 1)
                    if parent_field not in joined_fields:
                        raise Exception(
                            f"Parent field {parent_field} needs to be joined before joining {child_field} field. Raised while joining {field_name} for model {self.model.__name__}.")
                    parent_model = joined_fields[parent_field]
                else:
                    parent_model = self.model
                    child_field = field_name
                parent_table_name = re.sub(r'(?<!^)(?=[A-Z])', '_', parent_model.__name__).lower()
                join_field = next((field for field in fields(parent_model) if field.name == child_field), None)
                if join_field is None or join_field.metadata.get('field_type') != 'entity_id':
                    raise Exception(f"Invalid join field {child_field} specified for model {parent_model.__name__}.")
                join_model = join_field.metadata.get('relationship').get('model')
                join_table_name = re.sub(r'(?<!^)(?=[A-Z])', '_', join_model.__name__).lower()
                join_stmt_list.append(
                    f'INNER JOIN {join_table_name} ON {parent_table_name}.{child_field}={join_table_name}.entity_id AND {join_table_name}.active=true')
                join_field_list = [
                    f'{join_table_name}.{_field.name} AS joined_{child_field}_{join_table_name}_{_field.name}' for
                    _field in fields(join_model)]
                additional_fields += join_field_list
                joined_fields[field_name] = join_model

        if conditions:
            for condition_name, value in conditions.copy().items():
                condition_field = next((field for field in fields(self.model) if field.name == condition_name), None)
                if condition_field and condition_field.metadata.get('field_type') == 'entity_id':
                    if condition_name == 'entity_id':
                        condition_name = 'id'
                        conditions[condition_name] = conditions.pop('entity_id')

                    if isinstance(value, VersionedModel):
                        conditions[condition_name] = str(value.entity_id).replace('-', '')
                    elif isinstance(value, (str, UUID)):
                        conditions[condition_name] = str(value).replace('-', '')
                    elif isinstance(value, list):
                        # Handle list
                        conditions[condition_name] = []
                        for v in value:
                            if isinstance(v, VersionedModel):
                                conditions[condition_name].append(str(v.entity_id).replace('-', ''))
                            elif isinstance(v, (str, UUID)):
                                conditions[condition_name].append(str(v).replace('-', ''))
                            else:
                                raise NotImplementedError
                    else:
                        raise NotImplementedError

        records = self._execute_within_context(
            self.adapter.get_many, self.table_name, conditions, sort, limit, offset, join_statements=join_stmt_list,
            additional_fields=additional_fields
        )

        # If the adapter returned a single dictionary, wrap it in a list
        if isinstance(records, dict):
            records = [records]

        self.model()  # Calls __post_init__ of model to import related models and update fields.

        self._process_data_from_db(records)

        return [self.model.from_dict(record) for record in records]
