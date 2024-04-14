import logging
from typing import Any, Dict, List, Tuple, Union, Optional
from uuid import UUID

import pymysql

from rococo.data.base import DbAdapter


class MySqlAdapter(DbAdapter):
    """MySQL adapter for interacting with MySQL."""

    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._cursor_class = pymysql.cursors.DictCursor
        self._connection = None
        self._cursor = None

    def __enter__(self):
        """Context manager entry point for creating DB connection."""
        self._connection = pymysql.connect(
            host=self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            database=self._database,
            cursorclass=self._cursor_class
        )
        self._cursor = self._connection.cursor()

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Context manager exit point for closing DB connection."""

        if self._cursor is not None:
            self._cursor.close()
            self._cursor = None

        if self._connection is not None:
            self._connection.close()
            self._connection = None

    @property
    def connect(self):
        return pymysql.connect(
            host=self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            database=self._database,
            cursorclass=self._cursor_class
        )

    def _call_cursor(self, function_name, *args, **kwargs):
        """Calls a function specified by function_name argument in MySQL Cursor passing forward args and kwargs."""
        if not self._cursor:
            raise Exception("No cursor is available.")
        return getattr(self._cursor, function_name)(*args, **kwargs)

    def _build_condition_string(self, table, key, value):
        if '.' not in key:
            key = f"{table}.{key}"
        if isinstance(value, str):
            return f"{key}='{value}'"
        elif isinstance(value, bool):
            return f"{key}={'true' if value is True else 'false'}"
        elif type(value) in [int, float]:
            return f"{key}={value}"
        elif isinstance(value, list):
            return f"""{key} IN ({','.join(f'"{v}"' for v in value)})"""
        elif isinstance(value, UUID):
            return f"{key}='{str(value)}'"
        elif isinstance(value, type(None)):
            return f"{key} IS NULL"
        else:
            raise Exception(f"Unsupported type {type(value)} for condition key: {key}, value: {value}")

    def get_move_entity_to_audit_table_query(self, table, entity_id):
        """Returns the query to move an entity to audit table."""
        return f"""INSERT INTO {table}_audit (SELECT * FROM {table} WHERE entity_id=%s)""", (str(entity_id).replace('-', ''),)

    def move_entity_to_audit_table(self, table, entity_id):
        """Executes the query to move an entity to audit table."""
        query, values = self.get_move_entity_to_audit_table_query(table, entity_id)
        self._cursor.execute(query, values)
        self._connection.commit()

    def execute_query(self, sql, _vars=None):
        """Executes a query against the DB."""
        if _vars is None:
            _vars = {}

        self._call_cursor('execute', sql, _vars)
        return self._call_cursor('fetchall')

    def run_transaction(self, queries_list):
        """Executes a list of queries in a single transaction against the database."""
        for query in queries_list:
            if type(query) is tuple:
                query, values = query
            else:
                values = ()
            self._cursor.execute(query, values)
        self._connection.commit()

    def parse_db_response(self, response: List[Dict[str, Any]]) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Parse the response from SurrealDB.

        If the 'result' list has one item, return that item.
        If the 'result' list has multiple items, return the list.
        If the 'result' list is empty or if there's no 'result', return an empty list.
        """
        if not response or not isinstance(response, list):
            return []

        return response

    def get_one(
            self,
            table: str,
            conditions: Dict[str, Any],
            sort: List[Tuple[str, str]] = None,
            join_statements: list = None,
            additional_fields: list = None
    ) -> Optional[Dict[str, Any]]:
        fields = [f'{table}.*']
        if additional_fields:
            fields += additional_fields

        query = f"SELECT {', '.join(fields)} FROM {table}"
        for join_stmt in join_statements:
            query += f"""\n{join_stmt}\n"""

        condition_strs = []
        if conditions:
            condition_strs = [f"{self._build_condition_string(table, k, v)}" for k, v in conditions.items()]
        condition_strs.append(f"{table}.active=true")
        query += f" WHERE {' AND '.join(condition_strs)}"

        if sort:
            sort_strs = [f"{column} {direction}" for column, direction in sort]
            query += f" ORDER BY {', '.join(sort_strs)}"
        query += " LIMIT 1"

        db_response = self.parse_db_response(self.execute_query(query))
        if not db_response:
            return None
        elif isinstance(db_response, list):
            return db_response[0]
        else:
            return db_response

    def get_many(
            self,
            table: str,
            conditions: Dict[str, Any] = None,
            sort: List[Tuple[str, str]] = None,
            limit: int = None,
            offset: int = None,
            active: bool = True,
            join_statements: list = None,
            additional_fields: list = None
    ) -> List[Dict[str, Any]]:

        fields = [f'{table}.*']
        if additional_fields:
            fields += additional_fields

        query = f"SELECT {', '.join(fields)} FROM {table}"
        for join_stmt in join_statements:
            query += f"""\n{join_stmt}\n"""

        condition_strs = []
        if conditions:
            condition_strs = [f"{self._build_condition_string(table, k, v)}" for k, v in conditions.items()]
        if active:
            condition_strs.append(f"{table}.active=true")
        if condition_strs:
            query += f" WHERE {' AND '.join(condition_strs)}"
        if sort:
            sort_strs = [f"{column} {direction}" for column, direction in sort]
            query += f" ORDER BY {', '.join(sort_strs)}"
        if limit is not None:
            query += f" LIMIT {limit}"
        if offset is not None:
            query += f" OFFSET {offset}"

        db_response = self.parse_db_response(self.execute_query(query))
        if not db_response:
            return []
        elif isinstance(db_response, dict):
            return [db_response]
        else:
            return db_response

    def get_save_query(self, table_name, data):
        """Returns a query to save an entity in database."""
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))

        values = tuple(data.values())
        query = f"REPLACE INTO {table_name} ({columns}) VALUES ({placeholders})"

        return query, values

    def _create_in_database(self, table_name, data):
        try:
            query, values = self.get_save_query(table_name, data)
            self._cursor.execute(query, values)
            self._connection.commit()
            return True
        except Exception as e:
            logging.error("Error in SQL:\n%s", e)
            raise e

    def save(self, table: str, data: Dict[str, Any]):
        self._create_in_database(table, data)
        return data

    def delete(self, table: str, data: Dict[str, Any]) -> bool:
        # Set active = false
        data['active'] = False
        self.save(table, data)
        return True
