"""PostgreSQLDbRepository class"""

from dataclasses import fields
from datetime import datetime
import json
import re
from typing import Any, Dict, List, Type, Union, Optional
from uuid import UUID

from rococo.data import PostgreSQLAdapter
from rococo.messaging import MessageAdapter
from rococo.models import VersionedModel
from rococo.repositories import BaseRepository


def adjust_conditions(conditions: Dict[str, Any]) -> Dict[str, Any]:
    """Convert UUIDs in the conditions dictionary to strings."""
    for key, value in conditions.items():
        if isinstance(value, list) and value and isinstance(value[0], UUID):
            conditions[key] = [str(id) for id in value]
    return conditions

class PostgreSQLRepository(BaseRepository):
    """PostgreSQLRepository class"""

    def __init__(
            self,
            db_adapter: PostgreSQLAdapter,
            model: Type[VersionedModel],
            message_adapter: MessageAdapter,
            queue_name: str,
            user_id: UUID = None
    ):
        super().__init__(db_adapter, model, message_adapter, queue_name, user_id=user_id)
        self.table_name = re.sub(r'(?<!^)(?=[A-Z])', '_', model.__name__).lower()
        self.model()

    def _process_data_before_save(self, instance: VersionedModel):
        """Method to convert VersionedModel instance to a data dictionary that can be saved to PostgreSQL"""
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

    def get_one(
        self, 
        conditions: Dict[str, Any] = None,
        fetch_related: List[str] = None
    ) -> Union[VersionedModel, None]:
        """get one"""

        if conditions is not None:
            conditions = adjust_conditions(conditions)

        data = self._execute_within_context(
            self.adapter.get_one, self.table_name, conditions
        )

        if not data:
            return None

        instance = self.model.from_dict(data)

        # Handle fetching related entities
        if fetch_related:
            related_instances = {}
            for related_field in fetch_related:
                if hasattr(instance, related_field):
                    related_entities = self.fetch_related_entities_for_field(
                        instance, related_field
                    )
                    if related_entities is not None:
                        related_instances[related_field] = related_entities
        
            # Replace related_instances in the main instance
            for key, value in related_instances.items():
                setattr(instance, key, value)

        return instance

    def get_many(
        self,
        conditions: Dict[str, Any] = None,
        sort: List[tuple] = None,
        limit: int = None,
        offset: int = None,
        fetch_related: List[str] = None
    ) -> List[VersionedModel]:
        """Get many records, with optional related fields fetched"""
        
        if conditions is not None:
            conditions = adjust_conditions(conditions)
        # Fetch the records
        records = self._execute_within_context(
            self.adapter.get_many, self.table_name, conditions, sort, limit, offset
        )

        # If the adapter returned a single dictionary, wrap it in a list
        if isinstance(records, dict):
            records = [records]

        # Create instances from the records
        instances = [self.model.from_dict(record) for record in records]

        # Handle fetching related entities for each instance
        if fetch_related:
            for instance in instances:
                related_instances = {}
                for related_field in fetch_related:
                    if hasattr(instance, related_field):
                        related_entities = self.fetch_related_entities_for_field(
                            instance, related_field
                        )
                        if related_entities is not None:
                            related_instances[related_field] = related_entities

                # Replace related_instances in the main instance
                for key, value in related_instances.items():
                    setattr(instance, key, value)

        return instances

    def fetch_related_entities_for_field(
        self, 
        instance: VersionedModel, 
        related_field: str
    ) -> Union[List, Optional[VersionedModel]]:
        """Fetch related entities for a given field in the instance."""
        
        related_value = getattr(instance, related_field)

        if related_value is None or (isinstance(related_value, list) and len(related_value) == 0):
            return None

        field_metadata = next((
            field.metadata for field in fields(instance) 
            if field.name == related_field
        ), None)

        if field_metadata and 'relationship' in field_metadata:
            relation_model = field_metadata['relationship']['model']
            relation_table_name = re.sub(r'(?<!^)(?=[A-Z])', '_', relation_model.__name__).lower()
            field_type = field_metadata['field_type']
            
            # Determine the relation type
            relation_type = field_metadata['relationship'].get('relation_type', None)
            relation_conditions = { f"{field_type}": related_value }
            relation_conditions = adjust_conditions(relation_conditions)

            if relation_type in ['one_to_many', 'many_to_many']:
                # Fetch multiple records
                related_records = self._execute_within_context(
                    self.adapter.get_many, relation_table_name, relation_conditions
                )

                if related_records is not None:
                    return [
                        relation_model.from_dict(rel_record) 
                        for rel_record in related_records
                    ]

            else:
                related_record = self._execute_within_context(
                    self.adapter.get_one, relation_table_name, relation_conditions
                )

                if related_record is not None:
                    return relation_model.from_dict(related_record)

            return None

        return None
