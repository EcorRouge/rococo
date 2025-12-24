import json
import logging
from datetime import datetime, timezone, timedelta
from uuid import UUID
from typing import Any, Dict, List, Optional, Type, Tuple
from rococo.data import MongoDBAdapter
from rococo.messaging import MessageAdapter
from rococo.repositories import BaseRepository
from rococo.models.versioned_model import BaseModel, VersionedModel, get_uuid_hex


class MongoDbRepository(BaseRepository):
    """Generic MongoDB repository for BaseModel with audit and messaging."""

    def __init__(
        self,
        db_adapter: MongoDBAdapter,
        model: Type[BaseModel],
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
        self.adapter: MongoDBAdapter = db_adapter
        self.logger = logging.getLogger(
            f"{__name__}.{self.__class__.__name__}")
        if not logging.getLogger().hasHandlers():
            logging.basicConfig(
                level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    def _process_data_before_save(
        self,
        instance: BaseModel,
        extra_data: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Prepares a BaseModel instance for saving by converting it into a data dictionary.

        This method prepares the instance for saving by updating its metadata with
        necessary information and converting it to a dictionary format suitable for
        MongoDB storage. It ensures that the `entity_id` is set as the MongoDB document
        `_id`.

        Args:
            instance (BaseModel): The instance of BaseModel to be processed.
            extra_data (Dict[str, Any], optional): Additional data to merge into the result
                (e.g., TTL fields for delete operations). Defaults to None.

        Returns:
            Dict[str, Any]: A dictionary representing the prepared data for MongoDB storage.
        """
        instance.prepare_for_save(changed_by_id=self.user_id)
        data = instance.as_dict(
            convert_datetime_to_iso_string=False,
            convert_uuids=True,
            export_properties=self.save_calculated_fields
        )

        data.pop("_id", None)

        # Only set latest flag for versioned models
        if self._is_versioned_model():
            data["latest"] = True

        # Apply any extra data (e.g., TTL fields for delete operations)
        if extra_data:
            data.update(extra_data)

        return data

    def get_one(
        self,
        collection_name: str,
        index: str,
        query: Dict[str, Any]
    ) -> Optional[BaseModel]:
        """
        Fetches a single record from a specified MongoDB collection based on the given query parameters and index.

        Args:
            collection_name (str): The name of the collection from which to fetch the record.
            index (str): The index to use for the query, providing a hint for optimization.
            query (Dict[str, Any]): A dictionary of query parameters to filter the records.

        Returns:
            Optional[VersionedModel]: An instance of the model if a matching record is found, otherwise None.
        """
        db_conditions = query.copy() if query else {}
        # Only add versioned model conditions for VersionedModel
        if self._is_versioned_model():
            if "latest" not in db_conditions:
                db_conditions["latest"] = True
            if "active" not in db_conditions:
                db_conditions["active"] = True

        data = self._execute_within_context(
            lambda: self.adapter.get_one(
                table=collection_name,
                conditions=db_conditions,
                hint=index
            )
        )

        if not data:
            return None

        return self.model.from_dict(data)

    def get_many(
        self,
        collection_name: str,
        index: str,
        query: Optional[Dict[str, Any]] = None,
        sort: Optional[List[Tuple[str, int]]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> List[BaseModel]:
        """
        Retrieves a list of records from a specified MongoDB collection based on the given query parameters and index.

        Args:
            collection_name (str): The name of the collection from which to fetch the records.
            index (str): The index to use for the query, providing a hint for optimization.
            query (Optional[Dict[str, Any]], optional): A dictionary of query parameters to filter the records. Defaults to None.
            limit (Optional[int], optional): The maximum number of records to retrieve. If None, no limit is applied. Defaults to None.
            offset (Optional[int], optional): The number of records to skip before returning results. If None, no offset is applied. Defaults to None.

        Returns:
            List[VersionedModel]: A list of model instances, each representing a record from the collection.
        """
        db_conditions = query.copy() if query else {}
        # Only add versioned model conditions for VersionedModel
        if self._is_versioned_model():
            if "latest" not in db_conditions:
                db_conditions["latest"] = True
            if "active" not in db_conditions:
                db_conditions["active"] = True

        records_data = self._execute_within_context(
            lambda: self.adapter.get_many(
                table=collection_name,
                conditions=db_conditions,
                hint=index,
                sort=sort,
                limit=limit,
                offset=offset
            )
        )

        if not records_data:
            return []

        result = []
        for data in records_data:
            result.append(self.model.from_dict(data))
        return result

    def delete(
        self,
        instance: BaseModel,
        collection_name: str
    ) -> BaseModel:
        """
        Deletes a BaseModel instance from the database.
        For VersionedModel, sets its active flag to False (soft delete).
        For non-versioned models (BaseModel), performs a hard delete from the database.

        Args:
            instance (BaseModel): The BaseModel instance to delete.
            collection_name (str): The name of the MongoDB collection to delete from.

        Returns:
            BaseModel: The deleted BaseModel instance.
        """
        self.logger.info(
            f"Deleting entity_id={getattr(instance, 'entity_id', 'N/A')} from {collection_name}")

        if self._is_versioned_model():
            # Soft delete for versioned models: set active=False and call save()
            instance.active = False

            # Build TTL data if configured
            extra_data = None
            if self.ttl_field:
                extra_data = {
                    self.ttl_field: datetime.now(timezone.utc) + timedelta(minutes=self.ttl_minutes)
                }

            return self.save(instance, collection_name, extra_data=extra_data)
        else:
            # Hard delete for non-versioned models
            with self.adapter:
                self.adapter.hard_delete(collection_name, instance.entity_id)
            return instance

    def aggregate(
        self,
        collection_name: str,
        pipeline: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Execute an aggregation pipeline and return raw results.

        This method executes a MongoDB aggregation pipeline on the specified collection
        and returns the unmodified results as a list of dictionaries. This is useful
        when you need raw aggregation results that may contain custom fields from
        $project stages or other transformations.

        Args:
            collection_name (str): The name of the collection to aggregate on.
            pipeline (List[Dict[str, Any]]): MongoDB aggregation pipeline stages.

        Returns:
            List[Dict[str, Any]]: Raw aggregation results.
        """

        return self._execute_within_context(
            lambda: self.adapter.aggregate(collection_name, pipeline)
        )

    def aggregate_objects(
        self,
        collection_name: str,
        pipeline: List[Dict[str, Any]]
    ) -> List[BaseModel]:
        """
        Execute an aggregation pipeline and return deserialized VersionedModel objects.

        This method is a wrapper around the aggregate method that deserializes the
        raw results into VersionedModel instances. Use this when your aggregation
        pipeline returns documents that can be converted to your model objects.

        Args:
            collection_name (str): The name of the collection to aggregate on.
            pipeline (List[Dict[str, Any]]): MongoDB aggregation pipeline stages.

        Returns:
            List[VersionedModel]: List of deserialized model instances.
        """
        raw_results = self.aggregate(collection_name, pipeline)

        if not raw_results:
            return []

        result = []
        for data in raw_results:
            result.append(self.model.from_dict(data))
        return result

    def create(
        self,
        instance: BaseModel,
        collection_name: str
    ) -> BaseModel:
        """
        Creates a new BaseModel instance in the database.

        This method is identical to :meth:`save`, except that it sets the `active` field to `True` 
        for VersionedModel before saving.

        :param instance: The BaseModel instance to save.
        :type instance: BaseModel
        :param collection_name: The name of the MongoDB collection to create in.
        :type collection_name: str
        :return: The saved BaseModel instance.
        :rtype: BaseModel
        """
        self.logger.info(
            f"Creating entity_id={getattr(instance, 'entity_id', 'N/A')} in {collection_name}")
        # Only set active for VersionedModel
        if self._is_versioned_model():
            instance.active = True
        return self.save(instance, collection_name)

    def create_many(
        self,
        instances: List[BaseModel],
        collection_name: str
    ) -> None:
        """
        Creates multiple BaseModel instances in the database.

        This method is identical to :meth:`create`, except that it takes a list of instances to create.

        :param instances: The list of BaseModel instances to create.
        :type instances: List[BaseModel]
        :param collection_name: The name of the MongoDB collection to insert into.
        :type collection_name: str
        """
        docs = []
        for instance in instances:
            instance.prepare_for_save(changed_by_id=self.user_id)
            # Only set active for VersionedModel
            if self._is_versioned_model():
                instance.active = True
            doc = instance.as_dict(
                convert_datetime_to_iso_string=True, convert_uuids=True)
            docs.append(doc)

        if docs:
            self.logger.info(
                f"Inserting {len(docs)} documents into {collection_name}")
            self._execute_within_context(
                lambda: self.adapter.insert_many(collection_name, docs)
            )

    def save(
        self,
        instance: BaseModel,
        collection_name: str,
        send_message: bool = False,
        extra_data: Dict[str, Any] = None
    ) -> BaseModel:
        """
        Saves a BaseModel instance to the database.

        Args:
            instance (BaseModel): The BaseModel instance to save.
            collection_name (str): The name of the MongoDB collection to save to.
            send_message (bool, optional): Whether to send a message to the message queue after saving. Defaults to False.
            extra_data (Dict[str, Any], optional): Additional data to merge into the save payload
                (e.g., TTL fields for delete operations). Defaults to None.

        Returns:
            BaseModel: The saved BaseModel instance.
        """
        # Prepare the data for saving
        payload = self._process_data_before_save(instance, extra_data=extra_data)
        
        # Use appropriate save method based on model type
        if self._is_versioned_model():
            # Determine if we should move to audit
            # Only move to audit if this is an update (not the first version) and audit is enabled
            move_to_audit = False
            if self.use_audit_table:
                previous_version = getattr(instance, 'previous_version', None)
                if previous_version and previous_version != get_uuid_hex(0):
                    move_to_audit = True
            
            saved = self._execute_within_context(
                lambda: self.adapter.save(collection_name, payload, move_to_audit=move_to_audit)
            )
        else:
            # For non-versioned models, use simple upsert
            saved = self._execute_within_context(
                lambda: self.adapter.upsert(collection_name, payload)
            )

        # Hydrate the returned fields onto our instance
        if saved:
            for k, v in saved.items():
                if hasattr(instance, k):
                    setattr(instance, k, v)

        # Send a message if requested
        if send_message:
            self.message_adapter.send_message(
                self.queue_name,
                json.dumps(instance.as_dict(
                    convert_datetime_to_iso_string=True))
            )

        return instance
