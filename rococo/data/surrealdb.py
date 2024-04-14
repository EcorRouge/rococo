import asyncio
from typing import Any, Dict, List, Tuple, Union
from uuid import UUID
from surrealdb import Surreal

from rococo.data.base import DbAdapter


class SurrealDbAdapter(DbAdapter):
    """SurrealDB adapter for interacting with SurrealDB."""

    def __init__(
        self, endpoint: str, username: str, password: str, namespace: str, db_name: str
    ):
        """Initializes a new SurrealDB adapter."""
        self._endpoint = endpoint
        self._username = username
        self._password = password
        self._namespace = namespace
        self._db_name = db_name
        self._db = None
        self._event_loop = None

    def __enter__(self):
        """Context manager entry point for preparing DB connection."""
        self._event_loop = asyncio.new_event_loop()
        self._db = self._event_loop.run_until_complete(self._prepare_db())
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Context manager exit point for closing DB connection."""
        self._event_loop.run_until_complete(self._db.close())
        self._event_loop.stop()
        self._event_loop = None
        self._db = None

    async def _prepare_db(self):
        """Prepares the DB connection."""
        db = Surreal(self._endpoint)
        await db.connect()
        await db.signin({"user": self._username, "pass": self._password})
        await db.use(self._namespace, self._db_name)
        return db

    def _call_db(self, function_name, *args, **kwargs):
        """Calls a function specified by function_name argument in SurrealDB connection passing forward args and kwargs."""
        if not self._db:
            raise Exception("No connection to SurrealDB.")
        return self._event_loop.run_until_complete(getattr(self._db, function_name)(*args, **kwargs))

    def _build_condition_string(self, key, value):
        if isinstance(value, str):
            return f"{key}='{value}'"
        elif isinstance(value, bool):
            return f"{key}={'true' if value is True else 'false'}"
        elif type(value) in [int, float]:
            return f"{key}={value}"
        elif isinstance(value, list):
            return f"""{key} IN [{','.join(f'"{v}"' for v in value)}]"""
        elif isinstance(value, UUID):
            return f"{key}='{str(value)}'"
        else:
            raise Exception(f"Unsuppported type {type(value)} for condition key: {key}, value: {value}")

    def run_transaction(self, operations_list: List[Any]):
        """Executes a list of operations against the database as a transaction."""
        # TODO: Update this method when SurrealDB Python SDK supports transactions.
        for operation in operations_list:
            self._call_db(*operation)

    def get_move_entity_to_audit_table_query(self, table, entity_id):
        """Returns the operation to move entity to an audit table."""
        return ('query', f'INSERT INTO {table}_audit (SELECT *, "{entity_id}" AS entity_id, rand::uuid::v4() AS id FROM {table} WHERE id={table}:`{entity_id}`)')

    def move_entity_to_audit_table(self, table, entity_id):
        """Executes a query to move entity to audit table."""
        move_entity_op = self.get_move_entity_to_audit_table_query(table, entity_id)
        self._call_db(*move_entity_op)

    def execute_query(self, sql, _vars=None):
        """Executes a query against the DB."""
        if _vars is None:
            _vars = {}

        return self._call_db('query', sql, _vars)

    def parse_db_response(self, response: List[Dict[str, Any]]) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Parse the response from SurrealDB.

        If the 'result' list has one item, return that item.
        If the 'result' list has multiple items, return the list.
        If the 'result' list is empty or if there's no 'result', return an empty list.
        """
        if not response or not isinstance(response, list):
            return []

        results = response[0].get("result", [])
        if len(results) == 1:
            return results[0]
        return results

    def get_one(
        self,
        table: str,
        conditions: Dict[str, Any],
        sort: List[Tuple[str, str]] = None,
        fetch_related: list = None,
        additional_fields: list = None
    ) -> Dict[str, Any]:
        fields = ['*']
        if additional_fields:
            fields += additional_fields

        query = f"SELECT {', '.join(fields)} FROM {table}"
        
        condition_strs = []
        if conditions:
            condition_strs = [f"{self._build_condition_string(k, v)}" for k, v in conditions.items()]
        condition_strs.append("active=true")
        query += f" WHERE {' AND '.join(condition_strs)}"

        if sort:
            sort_strs = [f"{column} {direction}" for column, direction in sort]
            query += f" ORDER BY {', '.join(sort_strs)}"
        query += " LIMIT 1"
        
        if fetch_related:
            query += f" FETCH {', '.join(field for field in fetch_related)}"

        db_response = self.parse_db_response(self.execute_query(query))

        return db_response

    def get_many(
        self,
        table: str,
        conditions: Dict[str, Any] = None,
        sort: List[Tuple[str, str]] = None,
        limit: int = 100,
        active: bool = True,
        fetch_related: list = None,
        additional_fields: list = None
    ) -> List[Dict[str, Any]]:
        
        fields = ['*']
        if additional_fields:
            fields += additional_fields

        query = f"SELECT {', '.join(fields)} FROM {table}"
        
        condition_strs = []
        if conditions:
            condition_strs = [f"{self._build_condition_string(k, v)}" for k, v in conditions.items()]
        if active:
            condition_strs.append("active=true")
        if condition_strs:
            query += f" WHERE {' AND '.join(condition_strs)}"
        if sort:
            sort_strs = [f"{column} {direction}" for column, direction in sort]
            query += f" ORDER BY {', '.join(sort_strs)}"
        query += f" LIMIT {limit}"

        if fetch_related:
            query += f" FETCH {', '.join(field for field in fetch_related)}"

        db_response = self.parse_db_response(self.execute_query(query))

        return db_response

    def get_save_query(self, table: str, data: Dict[str, Any]):
        """Returns operation to save a data record in the table."""
        return 'update', data['id'], data

    def save(self, table: str, data: Dict[str, Any]):
        save_op = self.get_save_query(table, data)
        db_result = self._call_db(*save_op)
        return db_result

    def delete(self, table: str, data: Dict[str, Any]) -> bool:
        # Set active = false
        data['active'] = False
        return self.save(table, data)
