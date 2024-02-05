"""Mysql Database Adapter"""
from typing import Any, Dict, List, Tuple, Union
from uuid import UUID
import pymysql
from rococo.data.base import DbAdapter

class MySqlDbAdapter(DbAdapter):
    """Mysql Adapter Class"""
    def __init__(self, host: str, user: str, password: str, database: str):
        self._host = host
        self._user = user
        self._password = password
        self._database = database
        self._connection = None
        self._cursor = pymysql.cursors.DictCursor


    def __enter__(self):
        self._connection = pymysql.connect(
            host=self._host,
            user=self._user,
            password=self._password,
            database=self._database,
            cursorclass=self._cursor
        )
        self._cursor = self._connection.cursor()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._cursor.close()
        self._connection.close()

    def execute_query(self, sql: str) -> Any: # pylint: disable=W0221
        self._cursor.execute(sql)
        self._connection.commit()
        return self._cursor.fetchall()

    def parse_db_response(self, response: List[Dict[str, Any]],  # pylint: disable=W0221
                          get_one:bool) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Parse the response from SurrealDB.

        If the 'response' list has one item, return that item.
        If the 'response' list has multiple items, return the list.
        If the 'response' list is empty or if there's no 'result', return an empty list.
        """
        if not response:
            if get_one:
                return {}
            return []
        if len(response) == 1:
            if not get_one:
                return [response]
            return response
        return response

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
            raise Exception( # pylint: disable=W0719
                f"Unsuppported type {type(value)} for condition key: {key}, value: {value}")

    def _build_update_string(self,key,value):
        if isinstance(value, str):
            return f"{key}='{value}'"
        elif isinstance(value, bool):
            return f"{key}={'true' if value is True else 'false'}"
        elif type(value) in [int, float]:
            return f"{key}={value}"
        elif isinstance(value, list):
            return f"""{key}=[{','.join(f'"{v}"' for v in value)}]"""
        elif isinstance(value, UUID):
            return f"{key}='{str(value)}'"
        else:
            raise Exception( # pylint: disable=W0719
                f"Unsuppported type {type(value)} for condition key: {key}, value: {value}")

    def get_one(
            self, table: str, conditions: Dict[str, Any],
            sort: List[Tuple[str, str]] = None, additional_fields: list = None
            ) -> Dict[str, Any]:
        fields = ['*']
        if additional_fields:
            fields += additional_fields

        query = f"SELECT {', '.join(fields)} FROM {table}"

        condition_strs = []
        if conditions:
            condition_strs = [
                f"{self._build_condition_string(k, v)}" for k, v in conditions.items()]
        condition_strs.append("active=true")
        query += f" WHERE {' AND '.join(condition_strs)}"

        if sort:
            sort_strs = [f"{column} {direction}" for column, direction in sort]
            query += f" ORDER BY {', '.join(sort_strs)}"
        query += " LIMIT 1"

        # TODO
        # if fetch_related:
            # query += f" FETCH {', '.join(field for field in fetch_related)}"

        db_response = self.parse_db_response(self.execute_query(query),get_one=True)

        return db_response

    def get_many(self, table: str, conditions: Dict[str, Any] = None, # pylint: disable=R0913
                 sort: List[Tuple[str, str]] = None,
                 limit: int = 100, additional_fields: list = None) -> List[Dict[str, Any]]:
        fields = ['*']
        if additional_fields:
            fields += additional_fields

        query = f"SELECT {', '.join(fields)} FROM {table}"

        condition_strs = []
        if conditions:
            condition_strs = [
                f"{self._build_condition_string(k, v)}" for k, v in conditions.items()]
        condition_strs.append("active=true")
        query += f" WHERE {' AND '.join(condition_strs)}"

        if sort:
            sort_strs = [f"{column} {direction}" for column, direction in sort]
            query += f" ORDER BY {', '.join(sort_strs)}"
        query += f" LIMIT {limit}"

        # TODO
        # if fetch_related:
            # query += f" FETCH {', '.join(field for field in fetch_related)}"

        db_response = self.parse_db_response(self.execute_query(query),get_one=False)

        return db_response

    def move_entity_to_audit_table(self, table: str, entity_id: str): # pylint: disable=W0237
        query = (
            f'INSERT INTO {table}_audit ('
            f'SELECT *, "{entity_id}" AS entity_id, rand::uuid::v4() AS id '
            f'FROM {table} WHERE entity_id={table}.`{entity_id}`)'
        )
        self.execute_query(query)

    def save(self, table: str, data: Dict[str, Any]):
        query = f"UPDATE {table} SET "
        for k,v in data.items():
            if k == "entity_id":
                continue
            update_string = self._build_update_string(key=k,value=v)
            query = query + update_string + ", "
        query = query[:-2] # remove trailing ", "
        query = query + f" WHERE entity_id={data['entity_id']}"
        db_result = self.execute_query(query)
        return db_result

    def delete(self, table: str, data: Dict[str, Any]) -> bool:
        # Set active = false
        data['active'] = False
        return self.save(
            table,
            data
        )
