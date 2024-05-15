"""MongoDbRepository class"""

from dataclasses import fields
from datetime import datetime
import json
from typing import Dict, List, Type
from uuid import UUID
import uuid
from pymongo import ReturnDocument

from rococo.data import MongoDBAdapter
from rococo.messaging import MessageAdapter
from rococo.models.versioned_model import VersionedModel
from rococo.repositories import BaseRepository


def get_uuid4_hex():
    return uuid.uuid4().hex

class MongoDbRepository(BaseRepository):
    """MongoDbRepository class"""
    def __init__(
            self,
            db_adapter: MongoDBAdapter,
            model: Type[VersionedModel],
            message_adapter: MessageAdapter,
            queue_name: str,
            user_id: UUID = None
    ):
        super().__init__(db_adapter, model, message_adapter, queue_name, user_id=user_id)
        self.db = db_adapter.db
        self.model()


    def _process_data_before_save(self, instance: VersionedModel):
        """Method to convert VersionedModel instance to a data dictionary that can be inserted in MongoDB"""
        super()._process_data_before_save(instance)
        data = instance.as_dict(convert_datetime_to_iso_string=False, convert_uuids=True)
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

    def get_move_entity_to_audit_table_query(self, data):
        instance = self.get_one(self.table_name, "entity_id", {'entity_id': data.get('entity_id')})
        self.db[f"{self.table_name}_audit"].insert_one(instance)
    
    def get_save_query(self, data):
        self.db[f"{self.table_name}"].get_one_and_update(
            {'entity_id': data.get("entity_id")},
            {'$set': data},
        )
    
    def save(self, instance: VersionedModel, send_message: bool = False):
        """Save func"""
        data = self._process_data_before_save(instance)
        with self.adapter:
            self.adapter.run_transaction([lambda: self.get_move_entity_to_audit_table_query(data), lambda: self.get_save_query(data)])
            if send_message:
                # This assumes that the instance is now in post-saved state with all the new DB updates
                message = json.dumps(instance.as_dict(convert_datetime_to_iso_string=True))
                self.message_adapter.send_message(self.queue_name, message)
        return instance

    def _insert(self, data: Dict, collection_name: str):
        self.db[collection_name].insert_one(data)

    def _outdate(self, data: Dict, collection_name: str):
        collection = self.db[collection_name]
        return collection.find_one_and_update(
            {'entity_id': data.get("entity_id"), 'latest': True, 'active': True},
            {'$set': {'latest': False}},
            upsert=False,
            return_document=ReturnDocument.AFTER
        )

    def create(self, data: Dict, collection_name: str):
        return self.update(data, collection_name)

    def create_many(self, data: List[Dict], collection_name: str):
        self.db[collection_name].insert_many(documents=data)

    def update(self, data: Dict, collection_name: str):
        with self.client.start_session() as session:
            with session.start_transaction():
                try:
                    updated_doc = self._outdate(data, collection_name)
                    if updated_doc:
                        data.update({
                            'previous_version': updated_doc.get('version'),
                            'version': get_uuid4_hex(),
                            'changed_on': datetime.utcnow().isoformat()
                        })
                    self._insert(data, collection_name)
                    return data
                except Exception as ex:
                    raise ex

    def delete(self, data: Dict, collection_name: str):
        data.update({'active': False})
        return self.update(data, collection_name)

    def get_one(self, collection_name: str, index: str, query: Dict):
        base_query = {'latest': True, 'active': True}
        if query:
            base_query.update(query)

        data = self.db[collection_name].find_one(base_query, hint=index)
        if data:
            return data
            

    def get_all(self, collection_name: str, index: str, query: Dict = None):
        base_query = {'latest': True, 'active': True}
        if query:
            base_query.update(query)

        data = self.db[collection_name].find(
            base_query,
            hint=index
        )
        if data:
            return data
        return []

    def get_count(self, collection_name: str, index: str, query: Dict):
        query.update(dict(latest=True, active=True))
        return self.db[collection_name].count_documents(query, hint=index)

    def create_index(self, collection_name: str, columns: List, index_name: str, partial_filter: Dict):
        self.db[collection_name].create_index(
            columns,
            name=index_name,
            partialFilterExpression=partial_filter
        )
