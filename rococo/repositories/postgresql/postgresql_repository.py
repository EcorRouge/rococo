"""PostgreSQLDbRepository class"""

import re
from uuid import UUID
from datetime import datetime
from dataclasses import fields
from typing import Any, Dict, List, Type, Union, Optional

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
        self.table_name = re.sub(
            r'(?<!^)(?=[A-Z])', '_', model.__name__).lower()
        self.model()

    def _process_data_before_save(self, instance: VersionedModel):
        """Method to convert VersionedModel instance to a data dictionary that can be saved to PostgreSQL"""
        super()._process_data_before_save(instance)
        data = instance.as_dict(
            convert_datetime_to_iso_string=False,
            convert_uuids=False,
            export_properties=self.save_calculated_fields
        )
        for field in fields(instance):
            if data.get(field.name) is None:
                continue

            field_value = data[field.name]

            if field.metadata.get('field_type') in ['entity_id', 'uuid']:
                if isinstance(field_value, VersionedModel):
                    field_value = str(field_value.entity_id).replace('-', '')
                elif isinstance(field_value, dict):
                    field_value = str(field_value.get(
                        'entity_id')).replace('-', '')
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

    def get_count(
        self,
        # collection_name: str, # Use self.table_name for consistency
        index: Optional[str] = None,  # index (for hint) can be optional
        query: Optional[Dict[str, Any]] = None  # query can be optional
    ) -> int:
        """
        Retrieves the count of records in the repository's table that match the given query parameters.
        The 'index' parameter is used as a hint via the 'options' argument to the adapter.

        Args:
            index (Optional[str], optional): The name of the index to use for the query (passed as a hint). Defaults to None.
            query (Optional[Dict[str, Any]], optional): Additional query parameters to filter the results. Defaults to None.

        Returns:
            int: The count of matching records.
        """
        # Prepare the actual conditions for the database query
        # Default conditions from BaseRepository
        db_conditions = {'latest': True, 'active': True}
        if query:  # Ensure query is not None before updating
            actual_query = adjust_conditions(
                query.copy())  # Adjust UUIDs in query
            db_conditions.update(actual_query)
        # Ensure adjust_conditions is called even if query is None but db_conditions has UUIDs (not in this default)
        else:
            db_conditions = adjust_conditions(db_conditions)

        # Prepare the options dictionary for the adapter
        adapter_options: Dict[str, Any] = {}
        if index:  # If an index (hint) is provided, add it to options
            adapter_options['hint'] = index

        # Call the adapter's get_count method with table, conditions, and options
        # self.adapter is PostgreSQLAdapter here.
        # PostgreSQLAdapter.get_count signature (from previous context):
        # def get_count(self, table: str, conditions: Dict[str, Any], options: Optional[Dict[str, Any]] = None) -> int:
        count = self._execute_within_context(
            self.adapter.get_count,  # type: ignore
            self.table_name,  # Use self.table_name for the repository's primary table
            db_conditions,
            options=adapter_options if adapter_options else None
        )
        return count

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
            relation_table_name = re.sub(
                r'(?<!^)(?=[A-Z])', '_', relation_model.__name__).lower()
            field_type = field_metadata['field_type']

            # Determine the relation type
            relation_type = field_metadata['relationship'].get(
                'relation_type', None)
            relation_conditions = {f"{field_type}": related_value}
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
