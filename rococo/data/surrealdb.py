import asyncio
from typing import Any, Dict, List, Tuple, Union

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

    def execute_query(self, sql, _vars=None):
        """Executes a query against the DB."""
        if _vars is None:
            _vars = {}

        if not self._db:
            raise Exception("No connection to SurrealDB.")
        return self._event_loop.run_until_complete(self._db.query(sql, _vars))

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
        self, table: str, conditions: Dict[str, Any], sort: List[Tuple[str, str]] = None
    ) -> Dict[str, Any]:
        query = f"SELECT * FROM {table}"
        if conditions:
            condition_strs = [f"{k}='{v}'" for k, v in conditions.items()]
            query += f" WHERE {' AND '.join(condition_strs)}"
        if sort:
            sort_strs = [f"{column} {direction}" for column, direction in sort]
            query += f" ORDER BY {', '.join(sort_strs)}"
        query += " LIMIT 1"

        db_response = self.parse_db_response(self.execute_query(query))

        return db_response

    def get_many(
        self,
        table: str,
        conditions: Dict[str, Any] = None,
        sort: List[Tuple[str, str]] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        query = f"SELECT * FROM {table}"
        if conditions:
            condition_strs = [f"{k}='{v}'" for k, v in conditions.items()]
            query += f" WHERE {' AND '.join(condition_strs)}"
        if sort:
            sort_strs = [f"{column} {direction}" for column, direction in sort]
            query += f" ORDER BY {', '.join(sort_strs)}"
        query += f" LIMIT {limit}"

        db_response = self.parse_db_response(self.execute_query(query))

        return db_response

    def save(self, table: str, data: Dict[str, Any]):
        columns = ", ".join(data.keys())
        values = ", ".join([f"'{v}'" for v in data.values()])
        query = f"INSERT INTO {table} ({columns}) VALUES ({values})"

        db_response = self.parse_db_response(self.execute_query(query))

        return db_response

    def delete(self, table: str, conditions: Dict[str, Any]) -> bool:
        # Construct the conditions string for the SQL query
        condition_strs = [f"{k}='{v}'" for k, v in conditions.items()]
        conditions_sql = " AND ".join(condition_strs)

        # Construct the DELETE SQL query
        query = f"DELETE FROM {table} WHERE {conditions_sql}"

        # Execute the DELETE query
        db_response = self.execute_query(query)

        success = self.parse_db_response(db_response)
        return success
