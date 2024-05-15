from typing import Any, List
from rococo.data.base import DbAdapter
from pymongo import MongoClient


class MongoDBAdapter(DbAdapter):
    """MongoDB adapter for interacting with MongoDB."""

    def __init__(self, mongo_uri: str, mongo_database: str):
        self.client = MongoClient(mongo_uri)
        self.db_name = mongo_database
        self.db = self.client.get_database(mongo_database)

    def move_entity_to_audit_table(self, table, entity_id):
        """Executes a query to move entity to audit table."""
        entity = self.get_one(table, "entity_id", {"entity_id": entity_id})
        self.db[f"{table}_audit"].insert_one(entity)
        return entity

    def run_transaction(self, operations_list: List[Any]):
        """Executes a list of operations against the database as a transaction."""
        with self.client.start_session() as session:
            with session.start_transaction():
                for operation in operations_list:
                    operation()
