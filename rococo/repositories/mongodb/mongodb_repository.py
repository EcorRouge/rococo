import json
import logging
from uuid import UUID
from typing import Any, Dict, List, Optional, Type
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
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        if not logging.getLogger().hasHandlers():
            logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

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
        data = instance.as_dict(convert_datetime_to_iso_string=True, convert_uuids=True)
        # Remove _id if it exists to prevent immutability error, then set it to entity_id
        if '_id' in data:
            del data['_id']
        data["_id"] = data["entity_id"]
        data["active"] = True
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
        db_conditions = {"active": True, "latest": True}
        db_conditions.update(query or {})

        data = self._execute_within_context(
            lambda: self.adapter.get_one(
                table=collection_name,
                conditions=db_conditions,
                hint=index
            )
        )

        if not data:
            return None

        data.setdefault('entity_id', str(data.get('_id')))
        return self.model.from_dict(data)

    def get_many(
        self,
        collection_name: str,
        index: str,
        query: Optional[Dict[str, Any]] = None,
        limit: int = 0
    ) -> List[VersionedModel]:
        """
        Retrieves a list of records from a specified MongoDB collection based on the given query parameters and index.

        Args:
            collection_name (str): The name of the collection from which to fetch the records.
            index (str): The index to use for the query, providing a hint for optimization.
            query (Optional[Dict[str, Any]], optional): A dictionary of query parameters to filter the records. Defaults to None.
            limit (int, optional): The maximum number of records to retrieve. If set to 0, the default limit of 100 is used. Defaults to 0.

        Returns:
            List[VersionedModel]: A list of model instances, each representing a record from the collection.
        """
        db_conditions = {'active': True}
        if query:
            db_conditions.update(query)

        actual_limit = limit if limit > 0 else 100

        records_data = self._execute_within_context(
            lambda: self.adapter.get_many(
                table=collection_name,
                conditions=db_conditions,
                hint=index,
                limit=actual_limit
            )
        )

        if not records_data:
            return []

        result = []
        for data in records_data:
            data.setdefault('entity_id', str(data.get('_id')))
            result.append(self.model.from_dict(data))
        return result

    def delete(
        self,
        instance: VersionedModel
    ) -> VersionedModel:
        """
        Logically deletes a VersionedModel instance from the database by setting its active flag to False.

        Args:
            instance (VersionedModel): The VersionedModel instance to delete.

        Returns:
            VersionedModel: The deleted VersionedModel instance, which is now in a logically deleted state.
        """
        self.logger.info(f"Deleting entity_id={getattr(instance, 'entity_id', 'N/A')} from {self.table_name}")

        instance.prepare_for_save(changed_by_id=self.user_id)
        instance.active = False

        data = instance.as_dict(convert_datetime_to_iso_string=True, convert_uuids=True)
        # Remove _id if it exists to prevent immutability error, then set it to entity_id
        if '_id' in data:
            del data['_id']
        data['_id'] = data.get('entity_id')

        if instance.previous_version and instance.previous_version != get_uuid_hex(0):
            self._execute_within_context(
                lambda: self.adapter.move_entity_to_audit_table(
                    self.table_name,
                    data['entity_id']
                )
            )

        saved = self._execute_within_context(
            lambda: self.adapter.save(
                self.table_name,
                data
            )
        )

        if saved:
            for k, v in saved.items():
                if hasattr(instance, k):
                    setattr(instance, k, v)
            if '_id' in saved:
                setattr(instance, 'id', str(saved['_id']))

        return instance

    def create(
        self,
        instance: VersionedModel
    ) -> VersionedModel:
        """
        Creates a new VersionedModel instance in the database.

        This method is identical to :meth:`save`, except that it sets the `active` field to `True` before saving.

        :param instance: The VersionedModel instance to save.
        :type instance: VersionedModel
        :return: The saved VersionedModel instance, which is now in a logically active state.
        :rtype: VersionedModel
        """
        self.logger.info(f"Creating entity_id={getattr(instance, 'entity_id', 'N/A')} in {self.table_name}")
        instance.active = True
        return self.save(instance)

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
            doc = instance.as_dict(convert_datetime_to_iso_string=True, convert_uuids=True)
            # Remove _id if it exists to prevent immutability error, then set it to entity_id
            if '_id' in doc:
                del doc['_id']
            doc['_id'] = doc.get('entity_id')
            docs.append(doc)

        if docs:
            self.logger.info(f"Inserting {len(docs)} documents into {collection_name}")
            self._execute_within_context(
                lambda: self.adapter.insert_many(collection_name, docs)
            )

    def save(
        self,
        instance: VersionedModel,
        send_message: bool = False
    ) -> VersionedModel:
        # 0) if this isn't the very first version, move the existing latest doc into audit
        if instance.previous_version and instance.previous_version != get_uuid_hex(0):
            self._execute_within_context(
                lambda: self.adapter.move_entity_to_audit_table(
                    self.table_name,
                    instance.entity_id
                )
            )
                
        # 1) Un-flag the old 'latest' document(s)
        def unset_old():
            # find any prior version marked latest
            old_docs = self.adapter.get_many(
                table=self.table_name,
                conditions={
                    "entity_id": instance.entity_id,
                    "latest": True,
                    "active": True
                },
                hint=None,
                limit=0
            )
            # mark them as no longer latest
            for old in old_docs:
                old["latest"] = False
                # write back the change
                self.adapter.save(self.table_name, old)

        self._execute_within_context(unset_old)

        # 2) Prepare & write the new version
        payload = self._process_data_before_save(instance)
        saved = self._execute_within_context(
            lambda: self.adapter.save(self.table_name, payload)
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
                json.dumps(instance.as_dict(convert_datetime_to_iso_string=True))
            )

        return instance