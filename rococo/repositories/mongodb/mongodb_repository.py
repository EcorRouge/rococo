"""MongoDbRepository class"""

import json
from uuid import UUID
from datetime import datetime
from typing import Any, Dict, List, Optional, Type
from dataclasses import fields as dataclass_fields
from rococo.messaging import MessageAdapter
from rococo.repositories import BaseRepository
from rococo.data.mongodb import MongoDBAdapter
from rococo.models.versioned_model import VersionedModel

class MongoDbRepository(BaseRepository):
    """Generic MongoDB repository for VersionedModel with audit and messaging."""

    def __init__(
        self,
        db_adapter: MongoDBAdapter,
        model: Type[VersionedModel],
        message_adapter: MessageAdapter,
        queue_name: str,
        user_id: Optional[UUID] = None
    ):
        """
        Initializes a MongoDbRepository instance.

        Args:
            db_adapter (MongoDBAdapter): The database adapter for MongoDB operations.
            model (Type[VersionedModel]): The model class to be used in this repository.
            message_adapter (MessageAdapter): The message adapter for messaging operations.
            queue_name (str): The name of the message queue.
            user_id (Optional[UUID], optional): The user ID associated with operations. Defaults to None.
        """
        super().__init__(
            db_adapter,
            model,
            message_adapter,
            queue_name,
            user_id=user_id
        )

    def _process_data_before_save(self, instance: VersionedModel) -> Dict[str, Any]:
        """
        Convert a VersionedModel instance into a data dictionary for MongoDB storage.
        This method processes the data from the given instance, converting it into
        a format suitable for saving in MongoDB. It handles the conversion of UUIDs
        by stripping dashes and formats datetime objects into strings. The method
        iterates over each field of the instance, checking for specific metadata 
        to apply the appropriate transformations.

        Args:
            instance (VersionedModel): The instance to be processed.

        Returns:
            Dict[str, Any]: A dictionary representation of the instance, with UUIDs
            and datetime objects appropriately formatted.
        """
        super()._process_data_before_save(instance)
        data = instance.as_dict(
            convert_datetime_to_iso_string=False,
            convert_uuids=True
        )
        for f in dataclass_fields(instance):
            val = data.get(f.name)
            if val is None:
                continue
            # Strip dashes for UUID/entity_id fields
            if f.metadata.get('field_type') in ['entity_id', 'uuid']:
                val = str(val).replace('-', '')
            # Format datetimes
            if isinstance(val, datetime):
                val = val.strftime('%Y-%m-%d %H:%M:%S')
            data[f.name] = val
        return data

    def get_one(
        self,
        collection_name: str,
        index: str,
        query: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieves a single document matching the given query and index.

        Args:
            collection_name (str): The name of the MongoDB collection to query.
            index (str): The name of the index to use for the query.
            query (Dict[str, Any]): The additional query parameters to filter the results.

        Returns:
            Optional[Dict[str, Any]]: The single document matching the query, or None if no document is found.
        """
        base = {'latest': True, 'active': True}
        base.update(query or {})
        return self._execute_within_context(
            lambda: self.adapter.get_one(
                collection_name,
                base,
                hint=index
            )
        )

    def get_many(
        self,
        collection_name: str,
        index: str,
        query: Dict[str, Any] = None,
        limit: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Retrieves multiple documents from a specified MongoDB collection
        matching the given query parameters, index, and limit.

        Args:
            collection_name (str): The name of the MongoDB collection to query.
            index (str): The name of the index to use for the query.
            query (Dict[str, Any], optional): Additional query parameters to filter the results.
            limit (int, optional): The maximum number of documents to retrieve. Defaults to 0 for no limit.

        Returns:
            List[Dict[str, Any]]: A list of documents matching the query parameters.
        """
        base = {'latest': True, 'active': True}
        if query:
            base.update(query)
        return self._execute_within_context(
            lambda: self.adapter.get_many(
                collection_name,
                base,
                hint=index,
                limit=limit
            )
        )

    def count(
        self,
        collection_name: str,
        index: str,
        query: Dict[str, Any]
    ) -> int:
        """
        Count the number of documents in a collection that match the query.

        Args:
            collection_name (str): The name of the collection to query.
            index (str): The name of the index to use for the query.
            query (Dict[str, Any], optional): The query parameters to filter the results.

        Returns:
            int: The number of matching documents.
        """
        base = {'latest': True, 'active': True}
        base.update(query or {})
        return self.get_count(collection_name, index, query)

    def delete(
        self,
        instance: VersionedModel,
        collection_name: str
    ) -> VersionedModel:
        """
        Deactivates a given instance in the specified MongoDB collection.

        Args:
            instance (VersionedModel): The instance to deactivate.
            collection_name (str): The name of the MongoDB collection to query.

        Returns:
            VersionedModel: The deactivated instance.
        """
        instance.active = False
        return self.save(instance)

    def create(
        self,
        instance: VersionedModel,
        collection_name: str
    ) -> VersionedModel:
        """
        Creates a new document in the specified MongoDB collection.

        Args:
            instance (VersionedModel): The instance to save.
            collection_name (str): The name of the MongoDB collection to query.

        Returns:
            VersionedModel: The saved instance.
        """
        instance.active = True
        instance.latest = True
        return self.save(instance)

    def create_many(
        self,
        instances: List[VersionedModel],
        collection_name: str
    ) -> None:
        """
        Creates multiple documents in the specified MongoDB collection.

        Args:
            instances (List[VersionedModel]): A list of instances to save.
            collection_name (str): The name of the MongoDB collection to query.
        """
        docs = [
            self._process_data_before_save(
                inst.__class__(**inst.__dict__)
            )
            for inst in instances
        ]
        if docs:
            self._execute_within_context(
                lambda: self.adapter.insert_many(collection_name, docs)
            )

    def save(
        self,
        instance: VersionedModel,
        send_message: bool = False
    ) -> VersionedModel:
        """
        Saves the given instance to the specified MongoDB collection.

        Args:
            instance (VersionedModel): The instance to save.
            send_message (bool, optional): If True, sends a message to the
                configured message queue with the saved instance as a JSON
                payload. Defaults to False.

        Returns:
            VersionedModel: The saved instance.

        Notes:
            Will move the previous version of the instance to the audit table
            if the instance is not new.
        """
        data = self._process_data_before_save(instance)
        # Move old version to audit
        self._execute_within_context(
            lambda: self.adapter.move_entity_to_audit_table(
                self.table_name,
                data['entity_id']
            )
        )
        # Upsert new version
        self._execute_within_context(
            lambda: self.adapter.save(
                self.table_name,
                data
            )
        )
        # Send message if requested
        if send_message:
            payload = json.dumps(
                instance.as_dict(
                    convert_datetime_to_iso_string=True
                )
            )
            self.message_adapter.send_message(
                self.queue_name,
                payload
            )
        return instance