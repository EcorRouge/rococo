from typing import Any, Dict, List, Tuple, Union
from rococo.data.base import DbAdapter
from pymongo import MongoClient


class MongoDBAdapter(DbAdapter):
    """MongoDB adapter for interacting with MongoDB."""

    def __init__(self, mongo_uri: str, mongo_database: str):
        self.client = MongoClient(mongo_uri)
        self.db_name = mongo_database

    def __enter__(self):
        """Context manager entry point for preparing DB connection."""
        self.db = self.client.get_database(self.db_name)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.client.close()

    def move_entity_to_audit_table(self, table, entity_id):
        """Executes a query to move entity to audit table."""
        entity = self.get_one(table, "entity_id", {"entity_id": entity_id})
        self.db[f"{table}_audit"].insert_one(entity)
        return entity

    def _call_db(self, function_name, *args, **kwargs):
        """Calls a function specified by function_name argument in MongoDB connection passing forward args and kwargs."""
        pass

    def run_transaction(self, operations_list: List[Any]):
        """Executes a list of operations against the database as a transaction."""
        with self.client.start_session() as session:
            with session.start_transaction():
                for operation in operations_list:
                    operation()

    def execute_query(self, sql: str, _vars = None) -> Any:
        """Executes a raw SQL query against the DB."""
        pass

    def parse_db_response(self, response: Any) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Parses the raw response from the database and returns structured data."""
        pass

    def get_one(self, table: str, conditions: Dict[str, Any], sort: List[Tuple[str, str]] = None) -> Dict[str, Any]:
        """Fetches a single record from the specified table based on given conditions."""
        pass

    def get_many(self, table: str, conditions: Dict[str, Any] = None, sort: List[Tuple[str, str]] = None, 
                 limit: int = 100) -> List[Dict[str, Any]]:
        """Fetches multiple records from the specified table based on given conditions."""
        pass

    def get_move_entity_to_audit_table_query(self, table, entity_id):
        """Returns query to move entity by entity_id to audit table."""
        pass

    def get_save_query(self, table: str, data: Dict[str, Any]):
        """Returns query to save a data record in the table."""
        pass

    def save(self, table: str, data: Dict[str, Any]) -> Union[Dict[str, Any], None]:
        """Saves or updates a record in the specified table."""
        pass

    def delete(self, table: str, data: Dict[str, Any]) -> bool:
        """Deletes a record in the specified table based on given conditions."""
        pass
