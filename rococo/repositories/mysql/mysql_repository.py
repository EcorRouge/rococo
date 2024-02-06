"""SurrealDbRepository class"""

from dataclasses import fields
from typing import Any, Dict, List, Type, Union
from uuid import UUID

from rococo.data import MySqlDbAdapter
from rococo.messaging import MessageAdapter
from rococo.models.mysql import VersionedModel
from rococo.repositories.mysql import BaseRepository


class MysqlRepository(BaseRepository):
    """MysqlRepository class"""
    def __init__(
            self,
            db_adapter: MySqlDbAdapter,
            model: Type[VersionedModel],
            message_adapter: MessageAdapter,
            queue_name: str,
            user_id: UUID = None
    ):
        super().__init__(db_adapter, model, message_adapter, queue_name, user_id=user_id)
        self.model()

    def _extract_uuid_from_str(self,string):
        try:
            return UUID(string)
        except Exception:
            return string

    def _process_data_before_save(self, instance: VersionedModel):
        """Method to convert VersionedModel instance to a data dictionary that can be inserted in SurrealDB"""
        data = super()._process_data_before_save(instance)

        for field in fields(instance):
            if data.get(field.name) is None:
                continue
            if field.metadata.get('field_type') == 'record_id':
                field_model_class = field.metadata.get('relationship', {}).get('model') or self.model
                field_table = field_model_class.__name__.lower()
                field_value = data[field.name]
                adapter_field_name = 'id' if field.name == 'entity_id' else field.name
                if isinstance(field_value, VersionedModel):
                    field_value = field_value.entity_id
                elif isinstance(field_value, dict):
                    field_value = field_value.get('entity_id')
                data[adapter_field_name] = f'{field_table}:`{field_value}`' if field_value else None

        data.pop('entity_id', None)
        return data

    def _process_data_from_db(self, data):
        """Method to convert data dictionary fetched from MySQL to a VersionedModel instance."""

        def _process_record(data: dict, model):
            model()
            for field in fields(model):
                if data.get(field.name) is None:
                    continue

                if field.metadata.get('field_type') == 'record_id':
                    field_model_class = field.metadata.get('relationship', {}).get('model') or model
                    field_value = data[field.name]

                    if isinstance(field_value, list):
                        data[field.name] = [_process_record(obj, field_model_class) for obj in field_value]
                    if isinstance(field_value, dict):
                        data[field.name] = _process_record(field_value, field_model_class)
                    elif isinstance(field_value, str):
                        field_uuid = self._extract_uuid_from_str(field_value)
                        if field.name == 'entity_id':
                            data[field.name] = field_uuid
                        else:
                            field_value = field_model_class(entity_id=field_uuid, _is_partial=True)
                            data[field.name] = field_value
                    else:
                        raise NotImplementedError
            return model.from_dict(data)


        if data is None:
            return None
        elif isinstance(data, list):
            for record in data:
                _process_record(record, self.model)
        elif isinstance(data, dict):
            _process_record(data, self.model)
        else:
            raise NotImplementedError


    def get_one(self, conditions: Dict[str, Any], fetch_related: List[str] = None) -> Union[VersionedModel, None]:
        """get one"""
        additional_fields = []
        if conditions:
            for condition_name, value in conditions.copy().items():
                condition_field = next((field for field in fields(self.model) if field.name == condition_name), None)
                if condition_field and condition_field.metadata.get('field_type') == 'record_id':
                    if condition_name == 'entity_id':
                        condition_name = 'id'
                        conditions[condition_name] = conditions.pop('entity_id')

                    field_model = condition_field.metadata.get('relationship', {}).get('model', None) or self.model
                    if isinstance(value, VersionedModel):
                        conditions[condition_name] = f"{field_model.__name__.lower()}:`{str(value.entity_id)}`"
                    elif isinstance(value, (str, UUID)):
                        conditions[condition_name] = f"{field_model.__name__.lower()}:`{str(value)}`"
                    elif isinstance(value, list):
                        # Handle list
                        conditions[condition_name] = []
                        for v in value:
                            if isinstance(v, VersionedModel):
                                conditions[condition_name].append(f"{field_model.__name__.lower()}:`{str(v.entity_id)}`")
                            elif isinstance(v, (str, UUID)):
                                conditions[condition_name].append(f"{field_model.__name__.lower()}:`{str(v)}`")
                            else:
                                raise NotImplementedError
                    else:
                        raise NotImplementedError


        data = self._execute_within_context(
            self.adapter.get_one, self.table_name, conditions, fetch_related=fetch_related, additional_fields=additional_fields
        )

        self.model()  # Calls __post_init__ of model to import related models and update fields.

        self._process_data_from_db(data)

        if not data:
            return None
        return self.model.from_dict(data)


    def get_many(
        self,
        conditions: Dict[str, Any] = None,
        sort: List[tuple] = None,
        limit: int = 100,
        fetch_related: List[str] = None,
    ) -> List[VersionedModel]:
        """get many"""
        additional_fields = []
        if conditions:
            for condition_name, value in conditions.copy().items():
                condition_field = next((field for field in fields(self.model) if field.name == condition_name), None)
                if condition_field and condition_field.metadata.get('field_type') == 'record_id':
                    if condition_name == 'entity_id':
                        condition_name = 'id'
                        conditions[condition_name] = conditions.pop('entity_id')

                    field_model = condition_field.metadata.get('relationship', {}).get('model', None) or self.model
                    if isinstance(value, VersionedModel):
                        conditions[condition_name] = f"{field_model.__name__.lower()}:`{str(value.entity_id)}`"
                    elif isinstance(value, (str, UUID)):
                        conditions[condition_name] = f"{field_model.__name__.lower()}:`{str(value)}`"
                    elif isinstance(value, list):
                        # Handle list
                        conditions[condition_name] = []
                        for v in value:
                            if isinstance(v, VersionedModel):
                                conditions[condition_name].append(f"{field_model.__name__.lower()}:`{str(v.entity_id)}`")
                            elif isinstance(v, (str, UUID)):
                                conditions[condition_name].append(f"{field_model.__name__.lower()}:`{str(v)}`")
                            else:
                                raise NotImplementedError
                    else:
                        raise NotImplementedError

        records = self._execute_within_context(
            self.adapter.get_many, self.table_name, conditions, sort, limit, fetch_related=fetch_related, additional_fields=additional_fields
        )

        # If the adapter returned a single dictionary, wrap it in a list
        if isinstance(records, dict):
            records = [records]

        self.model()  # Calls __post_init__ of model to import related models and update fields.

        self._process_data_from_db(records)

        return [self.model.from_dict(record) for record in records]

