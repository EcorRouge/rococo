import json
import logging
from datetime import datetime, timezone, timedelta
from uuid import UUID
from typing import Any, Dict, List, Optional, Type, Tuple
from rococo.data import MongoDBAdapter
from rococo.messaging import MessageAdapter
from rococo.repositories import BaseRepository
from rococo.models.versioned_model import VersionedModel, get_uuid_hex


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
        self.adapter: MongoDBAdapter = db_adapter
        self.logger = logging.getLogger(
            f"{__name__}.{self.__class__.__name__}")
        if not logging.getLogger().hasHandlers():
            logging.basicConfig(
                level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    def _process_data_before_save(
        self,
        instance: VersionedModel
    ) -> Dict[str, Any]:
        """
        Prepares a VersionedModel instance for saving by converting it into a data dictionary.

        This method prepares the instance for saving by updating its metadata with
        necessary information and converting it to a dictionary format suitable for
        MongoDB storage. It ensures that the `entity_id` is set as the MongoDB document
        `_id`.

        Args:
            instance (VersionedModel): The instance of VersionedModel to be processed.

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

        data["latest"] = True
        return data

    def get_one(
        self,
        collection_name: str,
        index: str,
        query: Dict[str, Any]
    ) -> Optional[VersionedModel]:
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
    ) -> List[VersionedModel]:
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
        instance: VersionedModel,
        collection_name: str
    ) -> VersionedModel:
        """
        Logically deletes a VersionedModel instance from the database by setting its active flag to False.

        Args:
            instance (VersionedModel): The VersionedModel instance to delete.
            collection_name (str): The name of the MongoDB collection to delete from.

        Returns:
            VersionedModel: The deleted VersionedModel instance, which is now in a logically deleted state.
        """
        self.logger.info(
            f"Deleting entity_id={getattr(instance, 'entity_id', 'N/A')} from {collection_name}")

        instance.prepare_for_save(changed_by_id=self.user_id)
        instance.active = False

        data = instance.as_dict(
            convert_datetime_to_iso_string=True, convert_uuids=True, export_properties=self.save_calculated_fields)

        if self.ttl_field:
            data[self.ttl_field] = datetime.now(
                timezone.utc) + timedelta(minutes=self.ttl_minutes)

        if self.use_audit_table and instance.previous_version and instance.previous_version != get_uuid_hex(0):
            self._execute_within_context(
                lambda: self.adapter.move_entity_to_audit_table(
                    collection_name,
                    data['entity_id']
                )
            )

        saved = self._execute_within_context(
            lambda: self.adapter.save(
                collection_name,
                data
            )
        )

        if saved:
            for k, v in saved.items():
                if hasattr(instance, k):
                    setattr(instance, k, v)

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
    ) -> List[VersionedModel]:
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
        instance: VersionedModel,
        collection_name: str
    ) -> VersionedModel:
        """
        Creates a new VersionedModel instance in the database.

        This method is identical to :meth:`save`, except that it sets the `active` field to `True` before saving.

        :param instance: The VersionedModel instance to save.
        :type instance: VersionedModel
        :param collection_name: The name of the MongoDB collection to create in.
        :type collection_name: str
        :return: The saved VersionedModel instance, which is now in a logically active state.
        :rtype: VersionedModel
        """
        self.logger.info(
            f"Creating entity_id={getattr(instance, 'entity_id', 'N/A')} in {collection_name}")
        instance.active = True
        return self.save(instance, collection_name)

    def create_many(
        self,
        instances: List[VersionedModel],
        collection_name: str
    ) -> None:
        """
        Creates multiple VersionedModel instances in the database.

        This method is identical to :meth:`create`, except that it takes a list of instances to create.

        :param instances: The list of VersionedModel instances to create.
        :type instances: List[VersionedModel]
        :param collection_name: The name of the MongoDB collection to insert into.
        :type collection_name: str
        """
        docs = []
        for instance in instances:
            instance.prepare_for_save(changed_by_id=self.user_id)
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
        instance: VersionedModel,
        collection_name: str,
        send_message: bool = False
    ) -> VersionedModel:
        """
        Saves a VersionedModel instance to the database.

        Args:
            instance (VersionedModel): The VersionedModel instance to save.
            collection_name (str): The name of the MongoDB collection to save to.
            send_message (bool, optional): Whether to send a message to the message queue after saving. Defaults to False.

        Returns:
            VersionedModel: The saved VersionedModel instance.
        """
        # 1) if this isn't the very first version, move the existing latest doc into audit
        if self.use_audit_table and instance.previous_version and instance.previous_version != get_uuid_hex(0):
            self._execute_within_context(
                lambda: self.adapter.move_entity_to_audit_table(
                    collection_name,
                    instance.entity_id
                )
            )

        # 2) Prepare & write the new version
        payload = self._process_data_before_save(instance)
        saved = self._execute_within_context(
            lambda: self.adapter.save(collection_name, payload)
        )

        # 3) Hydrate the returned fields onto our instance
        if saved:
            for k, v in saved.items():
                if hasattr(instance, k):
                    setattr(instance, k, v)

        # 4) Send a message if requested
        if send_message:
            self.message_adapter.send_message(
                self.queue_name,
                json.dumps(instance.as_dict(
                    convert_datetime_to_iso_string=True))
            )

        return instance
