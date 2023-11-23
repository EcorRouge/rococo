"""SurrealDbRepository class"""

from dataclasses import fields
from typing import Type
from uuid import UUID

from rococo.data import SurrealDbAdapter
from rococo.messaging import MessageAdapter
from rococo.models import VersionedModel
from rococo.repositories import BaseRepository


class SurrealDbRepository(BaseRepository):
    """SurrealDbRepository class"""
    def __init__(
            self,
            db_adapter: SurrealDbAdapter,
            model: Type[VersionedModel],
            message_adapter: MessageAdapter,
            queue_name: str,
            user_id: UUID = None
    ):
        super().__init__(db_adapter, model, message_adapter, queue_name, user_id=user_id)


    def _extract_uuid_from_surreal_id(self, surreal_id, table_name):
        """
        Converts a SurrealDB ID to a UUID
        Example: 'organization:⟨c87616ac-e6ca-4d3e-9177-27db7d2ebca8⟩' -> UUID('c87616ac-e6ca-4d3e-9177-27db7d2ebca8')
        """
        prefix = f"{table_name}:⟨"
        suffix = "⟩"
        if surreal_id.startswith(prefix) and surreal_id.endswith(suffix):
            uuid_str = surreal_id[len(prefix):-len(suffix)]
            formatted_uuid = UUID(uuid_str)
            return formatted_uuid
        else:
            raise ValueError(f"Invalid input format or no UUID found in the input string: {surreal_id}")

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

                data[adapter_field_name] = f'{field_table}:`{field_value}`' if field_value else None

        data.pop('entity_id', None)
        return data

    def _process_data_from_db(self, data):
        """Method to convert data dictionary fetched from SurrealDB to a VersionedModel instance."""

        def _process_record(data: dict, model):
            data['entity_id'] = data.pop('id')
            for field in fields(model):
                if data.get(field.name) is None:
                    continue
                if field.metadata.get('field_type') == 'record_id':
                    field_model_class = field.metadata.get('relationship', {}).get('model') or model
                    field_table = field_model_class.__name__.lower()
                    field_value = data[field.name]

                    if isinstance(field_value, list):
                        data[field.name] = [_process_record(obj, field_model_class) for obj in field_value]
                    if isinstance(field_value, dict):
                        data[field.name] = _process_record(field_value, field_model_class)
                    elif isinstance(field_value, str):
                        field_uuid = self._extract_uuid_from_surreal_id(field_value, field_table)
                        if field.name == 'entity_id':
                            data[field.name] = field_uuid
                        else:
                            field_value = field_model_class(entity_id=field_uuid, _is_partial=True)
                            data[field.name] = field_value

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
