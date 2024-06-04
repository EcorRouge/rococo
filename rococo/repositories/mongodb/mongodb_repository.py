"""MongoDbRepository class"""

from dataclasses import fields
from datetime import datetime
import json
from typing import Dict, List, Type
from uuid import UUID
import uuid

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

    def get_move_entity_to_audit_table_query(self, table, entity_id):
        instance = self.get_one(table, "", {'entity_id': entity_id})
        if instance:
            self._insert({k:v for k,v in instance.items() if k != "_id"}, f"{table}_audit")
    
    def get_save_query(self, table, data):
        self.db[table].find_one_and_update(
            {'entity_id': data.get("entity_id")},
            {'$set': {k:v for k,v in data.items() if k != "_id"}},
            upsert=True
        )

    def save(self, instance: VersionedModel, send_message: bool = False):
        """Save func"""
        data = self._process_data_before_save(instance)
        with self.adapter:
            self.adapter.run_transaction([lambda: self.get_move_entity_to_audit_table_query(self.table_name, data.get("entity_id")), lambda: self.get_save_query(self.table_name, data)])
            if send_message:
                # This assumes that the instance is now in post-saved state with all the new DB updates
                message = json.dumps(instance.as_dict(convert_datetime_to_iso_string=True))
                self.message_adapter.send_message(self.queue_name, message)
        return instance

    def _insert(self, data: Dict, collection_name: str):
        with self.adapter:
            self.db[collection_name].insert_one(data)

    def create(self, data: VersionedModel, collection_name: str):
        data.active = True
        data.latest = True
        return self.update(data, collection_name)

    def create_many(self, data: List[VersionedModel], collection_name: str):
        documents = []
        for instance in data:
            instance.active = True
            instance.latest = True
            data = self._process_data_before_save(instance)
            documents.append(data)
        self.db[collection_name].insert_many(documents=documents)

    def update(self, data: VersionedModel, collection_name: str, updated_by=None):
        self.table_name = collection_name
        with self.client.start_session() as session:
            with session.start_transaction():
                if data:
                    self.user_id = updated_by
                    return self.save(data)

    def delete(self, data: VersionedModel, collection_name: str):
        data.active = False
        return self.update(data, collection_name)

    def get_one(self, collection_name: str, index: str, query: Dict):
        base_query = {'latest': True, 'active': True}
        if query:
            base_query.update(query)

        kwargs = {"hint": index} if index else {}
        with self.adapter:
            data = self.db[collection_name].find_one(base_query, **kwargs)
            if data:
                return data
            

    def get_all(self, collection_name: str, index: str, query: Dict = None):
        base_query = {'latest': True, 'active': True}
        if query:
            base_query.update(query)
        with self.adapter:
            data = self.db[collection_name].find(
                base_query, hint=index
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
