"""
base repository for rococo
"""
import json
from uuid import UUID
from typing import Any, Dict, List, Type, Union
from rococo.data.base import DbAdapter
from rococo.messaging.base import MessageAdapter
from rococo.models.versioned_model import VersionedModel


class BaseRepository:
    """
    BaseRepository class
    """

    def __init__(
        self,
        adapter: DbAdapter,
        model: Type[VersionedModel],
        message_adapter: MessageAdapter,
        queue_name: str = 'placeholder',
        user_id: UUID = None
    ):
        self.adapter = adapter
        self.message_adapter = message_adapter
        self.queue_name = queue_name
        self.model = model
        self.table_name = model.__name__.lower()
        self.user_id = user_id
        # Don't save calculated fields (properties) to database by default
        self.save_calculated_fields = False
        # Enable auditing by default
        self.use_audit_table = True
        # Ttl field for delete() method
        self.ttl_field = None
        # Ttl (in minutes) for deleted records
        self.ttl_minutes = 0

    def _execute_within_context(
        self,
        func,
        *args,
        **kwargs
    ):
        """Utility method to execute adapter methods within the context manager."""
        with self.adapter:
            return func(*args, **kwargs)

    def _process_data_before_save(
        self,
        instance: VersionedModel
    ) -> Dict[str, Any]:
        """Convert a VersionedModel instance to a data dictionary for the adapter."""
        instance.prepare_for_save(changed_by_id=self.user_id)
        return instance.as_dict(
            convert_datetime_to_iso_string=True,
            export_properties=self.save_calculated_fields
        )

    def _process_data_from_db(
        self,
        data: Any
    ):
        """Hook to process raw DB data (can be overridden by subclass)."""
        pass

    def get_one(
        self,
        conditions: Dict[str, Any],
        fetch_related: List[str] = None
    ) -> Union[VersionedModel, None]:
        """
        Fetches a single record from the specified table based on given conditions.

        :param conditions: filter conditions
        :param fetch_related: list of related fields to fetch
        :return: a VersionedModel instance if found, None otherwise
        """
        data = self._execute_within_context(
            self.adapter.get_one,
            self.table_name,
            conditions,
            fetch_related=fetch_related
        )

        self._process_data_from_db(data)

        if not data:
            return None
        return self.model.from_dict(data)

    def get_many(
        self,
        conditions: Dict[str, Any] = None,
        sort: List[tuple] = None,
        limit: int = 100,
        offset: int = 0,
        fetch_related: List[str] = None
    ) -> List[VersionedModel]:
        """
        Fetches multiple records from the specified table based on given conditions.

        :param conditions: filter conditions
        :param sort: sort order
        :param limit: maximum number of records to return
        :param offset: number of records to skip before returning results
        :param fetch_related: list of related fields to fetch
        :return: list of VersionedModel instances
        """
        records = self._execute_within_context(
            self.adapter.get_many,
            self.table_name,
            conditions,
            sort,
            limit,
            offset,
            fetch_related=fetch_related
        )

        if isinstance(records, dict):
            records = [records]

        self._process_data_from_db(records)

        return [self.model.from_dict(record) for record in records]

    def get_count(
        self,
        collection_name: str,
        index: str,
        query: Dict[str, Any]
    ) -> int:
        """
        Retrieves the count of records in a specified collection that match the given query parameters
        and index.

        Args:
            collection_name (str): The name of the collection to query.
            index (str): The name of the index to use for the query. hint actually work for MongoDB and ignore by other DBs
            query (Dict[str, Any], optional): Additional query parameters to filter the results.

        Returns:
            int: The count of matching records.
        """
        # The 'query' parameter directly represents the conditions for the count.
        # If an 'active' filter is needed, it should be included in the 'query' argument by the caller.
        db_conditions = query

        adapter_options: Dict[str, Any] = {}
        if index:
            adapter_options['hint'] = index

        return self._execute_within_context(
            self.adapter.get_count,
            collection_name,
            db_conditions,
            options=adapter_options if adapter_options else None
        )

    def save(
        self,
        instance: VersionedModel,
        send_message: bool = False
    ) -> VersionedModel:
        """
        Saves a VersionedModel instance to the database.

        :param instance: The VersionedModel instance to save.
        :param send_message: Whether to send a message to the message queue after saving. Defaults to False.
        :return: The saved VersionedModel instance.
        """
        data = self._process_data_before_save(instance)
        with self.adapter:
            move_entity_query = self.adapter.get_move_entity_to_audit_table_query(
                self.table_name, instance.entity_id)
            save_entity_query = self.adapter.get_save_query(
                self.table_name, data)
            self.adapter.run_transaction(
                [move_entity_query, save_entity_query])
        if send_message:
            # This assumes that the instance is now in post-saved state with all the new DB updates
            message = json.dumps(instance.as_dict(
                convert_datetime_to_iso_string=True))
            self.message_adapter.send_message(self.queue_name, message)

        return instance

    def delete(
        self,
        instance: VersionedModel
    ) -> VersionedModel:
        """
        Logically deletes a VersionedModel instance from the database by setting its active flag to False.

        :param instance: The VersionedModel instance to delete.
        :return: The deleted VersionedModel instance, which is now in a logically deleted state.
        """
        instance.active = False
        return self.save(instance)
