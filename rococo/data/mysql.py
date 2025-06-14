import time
import pymysql
import logging
from uuid import UUID
from typing import Any, Dict, List, Tuple, Union, Optional, Callable
from rococo.data.base import DbAdapter


class MySqlAdapter(DbAdapter):
    """MySQL adapter for interacting with MySQL."""

    def __init__(self, host: str, port: int, user: str, password: str, database: str, connection_resolver: Optional[Callable] = None, connection_closer: Optional[Callable] = None):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._cursor_class = pymysql.cursors.DictCursor
        self._connection = None
        self._cursor = None

        if connection_resolver is None:
            self._connection_resolver = pymysql.connect
        else:
            self._connection_resolver = connection_resolver

        self._connection_closer = connection_closer

    def __enter__(self):
        """Context manager entry point for creating DB connection."""
        self._connection = self.connect
        self._cursor = self._connection.cursor()

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Context manager exit point for closing DB connection."""
        self.close_connection()

    def close_connection(self):
        """Closes the connection and cursor."""

        if self._connection_closer:
            self._connection_closer(self)
        else:
            if self._cursor is not None:
                self._cursor.close()
                self._cursor = None

            if self._connection is not None:
                self._connection.close()
                self._connection = None

    @property
    def connect(self):
        return self._connection_resolver(
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
            return f"{key} = %s", [value]
        elif isinstance(value, bool):
            return f"{key} = %s", [1 if value else 0]
        elif isinstance(value, (int, float)):
            return f"{key} = %s", [value]
        elif isinstance(value, list):
            placeholders = ', '.join(['%s'] * len(value))
            return f"{key} IN ({placeholders})", value
        elif isinstance(value, UUID):
            return f"{key} = %s", [str(value)]
        elif value is None:
            return f"{key} IS NULL", []
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
        Parse the response from MySQL.

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
        if join_statements:
            for join_stmt in join_statements:
                query += f"""\n{join_stmt}\n"""

        condition_strs_values = []
        if conditions:
            condition_strs_values = [self._build_condition_string(table, k, v) for k, v in conditions.items()]
        condition_strs_values.append((f"{table}.active = %s", [1]))
        query += f" WHERE {' AND '.join([condition_str for condition_str, condition_value in condition_strs_values])}"

        if sort:
            sort_strs = [f"{column} {direction}" for column, direction in sort]
            query += f" ORDER BY {', '.join(sort_strs)}"
        query += " LIMIT 1"

        values = sum((condition_value for condition_str, condition_value in condition_strs_values), [])
        db_response = self.parse_db_response(self.execute_query(query, tuple(values)))

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
        if join_statements:
            for join_stmt in join_statements:
                query += f"""\n{join_stmt}\n"""

        condition_strs_values = []
        if conditions:
            condition_strs_values = [self._build_condition_string(table, k, v) for k, v in conditions.items()]
        if active:
            condition_strs_values.append((f"{table}.active = %s", [1]))

        if condition_strs_values:
            query += f" WHERE {' AND '.join([condition_str for condition_str, condition_value in condition_strs_values])}"
        
        if sort:
            sort_strs = [f"{column} {direction}" for column, direction in sort]
            query += f" ORDER BY {', '.join(sort_strs)}"
        if limit is not None:
            query += f" LIMIT {limit}"
        if offset is not None:
            query += f" OFFSET {offset}"

        values = sum((condition_value for condition_str, condition_value in condition_strs_values), [])
        db_response = self.parse_db_response(self.execute_query(query, tuple(values)))
        if not db_response:
            return []
        elif isinstance(db_response, dict):
            return [db_response]
        else:
            return db_response

    def get_count(
        self,
        table: str,
        conditions: Dict[str, Any],
        options: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        Count rows in `table` matching `conditions`.
        The 'options' parameter is included for interface compatibility.
        """
        safe_table_name = f"`{table}`"
        cond_clauses: List[str] = []
        params: List[Any] = []
        
        if conditions:
            for key, val in conditions.items():
                clause, vals_list = self._build_condition_string(table, key, val)
                cond_clauses.append(clause)
                params.extend(vals_list)

        where_clause = f"WHERE {' AND '.join(cond_clauses)}" if cond_clauses else ""
        sql = f"SELECT COUNT(*) AS `count` FROM {safe_table_name} {where_clause}"

        if options and 'hint' in options:
            self.logger.info(f"MySQLAdapter.get_count received hint option: {options['hint']}, but it's not directly applied in this generic manner for MySQL.")

        rows = self.execute_query(sql, tuple(params))
        
        if rows and isinstance(rows, list) and rows[0] is not None:
            return int(rows[0].get('count', 0))
        return 0

    def get_save_query(self, table_name, data):
        """Returns a query to save an entity in database."""
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))

        values = tuple(data.values())
        query = f"REPLACE INTO {table_name} ({columns}) VALUES ({placeholders})"

        return query, values

    def _create_in_database(self, table_name, data, retry_count=0):
        try:
            query, values = self.get_save_query(table_name, data)
            self._cursor.execute(query, values)
            self._connection.commit()
            return True
        except pymysql.MySQLError as ex:
            self._connection.rollback()
            if retry_count < 3 and ex.args[0] == 1213:
                # Deadlock detected
                logging.warning("Deadlock detected on table %s. Retrying in %d seconds. Attempt %d",
                            table_name, 2**retry_count, retry_count+1)
                time.sleep(2**retry_count)
                return self._create_in_database(table_name, data, retry_count=retry_count+1)
            else:
                logging.error("Error in SQL:\n%s", ex)
                raise ex

    def save(self, table: str, data: Dict[str, Any]):
        self._create_in_database(table, data)
        return data

    def delete(self, table: str, data: Dict[str, Any]) -> bool:
        # Set active = false
        data['active'] = False
        self.save(table, data)
        return True
