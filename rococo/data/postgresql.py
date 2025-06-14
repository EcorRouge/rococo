import json
import time
import logging
import psycopg2
from uuid import UUID
from typing import Any, Dict, List, Tuple, Union, Optional, Callable

from rococo.data.base import DbAdapter


class PostgreSQLAdapter(DbAdapter):
    """PostgreSQL adapter for interacting with PostgreSQL."""

    def __init__(self, host: str, port: int, user: str, password: str, database: str, connection_resolver: Optional[Callable] = None, connection_closer: Optional[Callable] = None):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._connection = None
        self._cursor = None

        if connection_resolver is None:
            self._connection_resolver = psycopg2.connect
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
            database=self._database
        )

    def _call_cursor(self, function_name, *args, **kwargs):
        """Calls a function specified by function_name argument in PostgreSQL Cursor passing forward args and kwargs."""
        if not self._cursor:
            raise Exception("No cursor is available.")
        return getattr(self._cursor, function_name)(*args, **kwargs)

    def _build_condition_string(self, table, key, value):
        if '.' not in key:
            key = f"{table}.{key}"
            
        if isinstance(value, str):
            return f"{key} = %s", [value]
        elif isinstance(value, bool):
            return f"{key} = %s", [value]
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
        
        # Execute the query
        self._call_cursor('execute', sql, _vars)
        
        # Check if the query is a SELECT statement to fetch results
        if sql.strip().upper().startswith("SELECT"):
            # Fetch column names
            column_names = [desc[0] for desc in self._cursor.description]

            # Map each row tuple to a dictionary using column names
            return [dict(zip(column_names, row)) for row in self._call_cursor('fetchall')]
        else:
            # For other queries (like CREATE, INSERT, UPDATE, DELETE), return None or a success message
            self._connection.commit()
            return None
    
    def run_transaction(self, queries_list):
        """Executes a list of queries in a single transaction against the database."""

        for query in queries_list:
            if type(query) is tuple:
                query, values = query
            else:
                values = ()

            transformed_values = []
            for value in values:
                if isinstance(value, dict):
                    transformed_values.append(json.dumps(value))  # Convert dict to JSON string
                else:
                    transformed_values.append(value)

            self._cursor.execute(query, transformed_values)
        self._connection.commit()

    def parse_db_response(self, response: List[Dict[str, Any]]) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Parse the response from PostgreSQL.

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
        condition_strs_values.append((f"{table}.active = %s", ['true']))
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
            condition_strs_values.append((f"{table}.active = %s", ["true"]))

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
        The 'options' parameter is included for interface compatibility; PostgreSQL hints
        are typically injected as SQL comments or via session parameters.
        """
        # Quote the table name for safety
        safe_table = f'"{table}"'
        where_clauses: List[str] = []
        params: List[Any] = []

        # Build WHERE clauses
        if conditions:
            for key, val in conditions.items():
                clause, vals = self._build_condition_string(table, key, val)
                where_clauses.append(clause)
                params.extend(vals)

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        sql = f'SELECT COUNT(*) AS count FROM {safe_table} {where_sql}'

        # Log any hint passed through options (Postgres doesn’t support generic hints here)
        if options and 'hint' in options:
            logging.info(
                "PostgreSQLAdapter.get_count received hint=%r; ignoring for generic COUNT().",
                options['hint']
            )

        rows = self.execute_query(sql, tuple(params))
        if isinstance(rows, list) and rows:
            return int(rows[0].get('count', 0) or 0)
        return 0

    def get_save_query(self, table_name, data):
        """Returns a query to update a row or insert a new one in PostgreSQL."""
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))

        # Prepare the update columns in the form 'column_name = EXCLUDED.column_name'
        update_columns = ', '.join([f"{col} = EXCLUDED.{col}" for col in data.keys()])
        
        # The first column will be used for update condition (non-unique column, can be any)
        unique_column = list(data.keys())[0]

        # The query will first try to update, and if no rows are updated, it will insert
        query = (
            f"WITH updated AS ("
            f"  UPDATE {table_name} "
            f"  SET {', '.join([f'{col} = %s' for col in data.keys()])} "
            f"  WHERE {unique_column} = %s "
            f"  RETURNING *"
            f") "
            f"INSERT INTO {table_name} ({columns}) "
            f"SELECT {placeholders} "
            f"WHERE NOT EXISTS (SELECT 1 FROM updated)"
        )
        
        # Prepare values: first, the update values, followed by the insert values
        update_values = tuple(data.values()) + (data[unique_column],)  # values for update and delete condition
        insert_values = tuple(data.values())  # values for insert

        # Combine the update and insert values
        values = update_values + insert_values

        return query, values

    def _create_in_database(self, table_name, data, retry_count=0):
        try:
            query, values = self.get_save_query(table_name, data)
            self._cursor.execute(query, values)
            self._connection.commit()
            return True
        except psycopg2.Error as ex:
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
