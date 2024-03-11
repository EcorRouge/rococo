"""SurrealDbRepository class"""

from dataclasses import fields
from typing import Any, Dict, List, Type, Union
from uuid import UUID

from rococo.data import SurrealDbAdapter
from rococo.messaging import MessageAdapter
from rococo.models.surrealdb import SurrealVersionedModel
from rococo.repositories import BaseRepository


class SurrealDbRepository(BaseRepository):
    """SurrealDbRepository class"""
    def __init__(
            self,
            db_adapter: SurrealDbAdapter,
            model: Type[SurrealVersionedModel],
            message_adapter: MessageAdapter,
            queue_name: str,
            user_id: UUID = None
    ):
        super().__init__(db_adapter, model, message_adapter, queue_name, user_id=user_id)
        self.model()

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

    def _process_data_before_save(self, instance: SurrealVersionedModel):
        """Method to convert VersionedModel instance to a data dictionary that can be inserted in SurrealDB"""
        super()._process_data_before_save(instance)
        data = instance.as_dict(convert_datetime_to_iso_string=True, convert_uuids=True)

        for field in fields(instance):
            if data.get(field.name) is None:
                continue
            if field.metadata.get('field_type') == 'record_id':
                field_model_class = field.metadata.get('relationship', {}).get('model') or self.model
                field_table = field_model_class.__name__.lower()
                field_value = data[field.name]
                adapter_field_name = 'id' if field.name == 'entity_id' else field.name
                if isinstance(field_value, SurrealVersionedModel):
                    field_value = field_value.entity_id
                elif isinstance(field_value, dict):
                    field_value = field_value.get('entity_id')
                elif isinstance(field_value, UUID):
                    field_value = str(field_value)
                data[adapter_field_name] = f'{field_table}:`{field_value}`' if field_value else None

        data.pop('entity_id', None)
        return data

    def _process_data_from_db(self, data):
        """Method to convert data dictionary fetched from SurrealDB to a SurrealVersionedModel instance."""

        def _process_record(data: dict, model):
            data['entity_id'] = data.pop('id')
            model()
            for field in fields(model):
                if data.get(field.name) is None:
                    continue

                if field.metadata.get('field_type') == 'm2m_list':
                    field_model_class = field.metadata.get('relationship', {}).get('model') or model
                    field_table = field_model_class.__name__.lower()
                    field_value = data[field.name]
                    if isinstance(field_value, list):
                        data[field.name] = [_process_record(obj, field_model_class) for obj in field_value]
                    else:
                        raise NotImplementedError

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


    def get_one(self, conditions: Dict[str, Any], fetch_related: List[str] = None) -> Union[SurrealVersionedModel, None]:
        """get one"""
        additional_fields = []
        if fetch_related:
            for field in fields(self.model):
                relationship = field.metadata.get('relationship', {})
                if relationship.get('type') == 'associative':
                    if field.name in fetch_related:
                        name = relationship.get('name')
                        edge = '<-' if relationship.get('direction') == 'in' else '->'
                        model = relationship.get('model')
                        if not isinstance(model, str):
                            model = model.__name__
                        additional_fields.append(f"(SELECT * FROM {edge}{name}{edge}{model.lower()}) AS {field.name}")
                        fetch_related.remove(field.name)

        if conditions:
            for condition_name, value in conditions.copy().items():
                condition_field = next((field for field in fields(self.model) if field.name == condition_name), None)
                if condition_field and condition_field.metadata.get('field_type') == 'record_id':
                    if condition_name == 'entity_id':
                        condition_name = 'id'
                        conditions[condition_name] = conditions.pop('entity_id')

                    field_model = condition_field.metadata.get('relationship', {}).get('model', None) or self.model
                    if isinstance(value, SurrealVersionedModel):
                        conditions[condition_name] = f"{field_model.__name__.lower()}:`{str(value.entity_id)}`"
                    elif isinstance(value, (str, UUID)):
                        conditions[condition_name] = f"{field_model.__name__.lower()}:`{str(value)}`"
                    elif isinstance(value, list):
                        # Handle list
                        conditions[condition_name] = []
                        for v in value:
                            if isinstance(v, SurrealVersionedModel):
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
    ) -> List[SurrealVersionedModel]:
        """get many"""
        additional_fields = []
        if fetch_related:
            for field in fields(self.model):
                relationship = field.metadata.get('relationship', {})
                if relationship.get('type') == 'associative':
                    if field.name in fetch_related:
                        name = relationship.get('name')
                        edge = '<-' if relationship.get('direction') == 'in' else '->'
                        model = relationship.get('model')
                        if isinstance(model, type):
                            model = model.__name__
                        additional_fields.append(f"(SELECT * FROM {edge}{name}{edge}{model.lower()}) AS {field.name}")
                        fetch_related.remove(field.name)

        if conditions:
            for condition_name, value in conditions.copy().items():
                condition_field = next((field for field in fields(self.model) if field.name == condition_name), None)
                if condition_field and condition_field.metadata.get('field_type') == 'record_id':
                    if condition_name == 'entity_id':
                        condition_name = 'id'
                        conditions[condition_name] = conditions.pop('entity_id')

                    field_model = condition_field.metadata.get('relationship', {}).get('model', None) or self.model
                    if isinstance(value, SurrealVersionedModel):
                        conditions[condition_name] = f"{field_model.__name__.lower()}:`{str(value.entity_id)}`"
                    elif isinstance(value, (str, UUID)):
                        conditions[condition_name] = f"{field_model.__name__.lower()}:`{str(value)}`"
                    elif isinstance(value, list):
                        # Handle list
                        conditions[condition_name] = []
                        for v in value:
                            if isinstance(v, SurrealVersionedModel):
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

    def relate(self, in_edge: SurrealVersionedModel, association_name: str, out_edge: SurrealVersionedModel):
        query = f"RELATE {in_edge.__class__.__name__.lower()}:`{in_edge.entity_id}`->{association_name}->{out_edge.__class__.__name__.lower()}:`{out_edge.entity_id}`"
        self._execute_within_context(
            self.adapter.execute_query,
            query            
        )

    def unrelate(self, in_edge: SurrealVersionedModel, association_name: str, out_edge: SurrealVersionedModel):
        query = f"DELETE FROM {association_name} WHERE in={in_edge.__class__.__name__.lower()}:`{in_edge.entity_id}` AND out={out_edge.__class__.__name__.lower()}:`{out_edge.entity_id}`"
        self._execute_within_context(
            self.adapter.execute_query,
            query
        )
