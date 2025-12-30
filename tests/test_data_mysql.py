"""
Tests for MySQL database adapter.

This module tests the MySqlAdapter class that provides MySQL connectivity
and operations for the rococo framework.
"""
import unittest
from unittest.mock import Mock, MagicMock, patch
from typing import Dict, Any, List
from uuid import UUID

import pymysql
from rococo.data.mysql import MySqlAdapter


# Test constants
TEST_HOST = "localhost"
TEST_PORT = 3306
TEST_USER = "test_user"
TEST_PASSWORD = "test_password"
TEST_DATABASE = "test_database"
TEST_TABLE = "test_table"
TEST_ENTITY_ID = "test_entity_123"


class TestMySqlAdapterInit(unittest.TestCase):
    """Test MySqlAdapter initialization."""

    def test_init_with_defaults(self):
        """
        Test that MySqlAdapter initializes with default connection resolver.

        Verifies:
        - Connection parameters are stored
        - Default connection resolver is pymysql.connect
        - DictCursor class is set
        - Connection is None initially
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        self.assertEqual(adapter._host, TEST_HOST)
        self.assertEqual(adapter._port, TEST_PORT)
        self.assertEqual(adapter._user, TEST_USER)
        self.assertEqual(adapter._password, TEST_PASSWORD)
        self.assertEqual(adapter._database, TEST_DATABASE)
        self.assertEqual(adapter._cursor_class, pymysql.cursors.DictCursor)
        self.assertEqual(adapter._connection_resolver, pymysql.connect)
        self.assertIsNone(adapter._connection)
        self.assertIsNone(adapter._cursor)

    def test_init_with_custom_resolver(self):
        """
        Test initialization with custom connection resolver.

        Verifies custom resolver is used instead of default.
        """
        custom_resolver = Mock()
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE,
            connection_resolver=custom_resolver
        )

        self.assertEqual(adapter._connection_resolver, custom_resolver)

    def test_init_with_custom_closer(self):
        """
        Test initialization with custom connection closer.

        Verifies custom closer is stored.
        """
        custom_closer = Mock()
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE,
            connection_closer=custom_closer
        )

        self.assertEqual(adapter._connection_closer, custom_closer)


class TestMySqlAdapterContextManager(unittest.TestCase):
    """Test MySqlAdapter context manager protocol."""

    @patch('rococo.data.mysql.pymysql.connect')
    def test_enter_creates_connection(self, mock_connect):
        """
        Test __enter__ creates connection and cursor.

        Verifies:
        - Connection is created with cursorclass parameter
        - Cursor is created
        - Returns self
        """
        mock_connection = MagicMock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        result = adapter.__enter__()

        mock_connect.assert_called_once_with(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE,
            cursorclass=pymysql.cursors.DictCursor
        )
        self.assertEqual(adapter._connection, mock_connection)
        self.assertEqual(adapter._cursor, mock_cursor)
        self.assertEqual(result, adapter)

    def test_exit_closes_connection(self):
        """
        Test __exit__ closes cursor and connection.

        Verifies close_connection is called.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        mock_cursor = Mock()
        mock_connection = Mock()
        adapter._cursor = mock_cursor
        adapter._connection = mock_connection

        adapter.__exit__(None, None, None)

        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()
        self.assertIsNone(adapter._cursor)
        self.assertIsNone(adapter._connection)

    def test_close_connection_with_custom_closer(self):
        """
        Test close_connection uses custom closer when provided.

        Verifies custom closer is called with adapter instance.
        """
        custom_closer = Mock()
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE,
            connection_closer=custom_closer
        )

        adapter.close_connection()

        custom_closer.assert_called_once_with(adapter)

    def test_close_connection_without_cursor(self):
        """
        Test close_connection when cursor is None.

        Verifies no error is raised.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        mock_connection = Mock()
        adapter._connection = mock_connection
        adapter._cursor = None

        adapter.close_connection()

        mock_connection.close.assert_called_once()


class TestMySqlAdapterConnect(unittest.TestCase):
    """Test connect property."""

    @patch('rococo.data.mysql.pymysql.connect')
    def test_connect_property(self, mock_connect):
        """
        Test that connect property calls connection resolver with parameters.

        Verifies all connection parameters are passed including cursorclass.
        """
        mock_connection = Mock()
        mock_connect.return_value = mock_connection

        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        result = adapter.connect

        mock_connect.assert_called_once_with(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE,
            cursorclass=pymysql.cursors.DictCursor
        )
        self.assertEqual(result, mock_connection)


class TestMySqlAdapterCallCursor(unittest.TestCase):
    """Test _call_cursor method."""

    def test_call_cursor_success(self):
        """
        Test _call_cursor executes cursor method.

        Verifies method is called with args and kwargs.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        mock_cursor = Mock()
        mock_cursor.execute.return_value = "result"
        adapter._cursor = mock_cursor

        result = adapter._call_cursor('execute', 'SELECT * FROM table', ('value',))

        mock_cursor.execute.assert_called_once_with('SELECT * FROM table', ('value',))
        self.assertEqual(result, "result")

    def test_call_cursor_no_cursor(self):
        """
        Test _call_cursor raises error when cursor is None.

        Verifies RuntimeError is raised.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )
        adapter._cursor = None

        with self.assertRaises(RuntimeError) as context:
            adapter._call_cursor('execute', 'SELECT * FROM table')

        self.assertIn("No cursor is available", str(context.exception))


class TestMySqlAdapterBuildCondition(unittest.TestCase):
    """Test _build_condition_string method."""

    def test_build_condition_string(self):
        """
        Test _build_condition_string with string value.

        Verifies condition and values are returned.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        condition, values = adapter._build_condition_string(TEST_TABLE, 'name', 'test_value')

        self.assertEqual(condition, f"{TEST_TABLE}.name = %s")
        self.assertEqual(values, ['test_value'])

    def test_build_condition_bool_true(self):
        """
        Test _build_condition_string with boolean True.

        Verifies bool is converted to 1 for MySQL.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        condition, values = adapter._build_condition_string(TEST_TABLE, 'active', True)

        self.assertEqual(condition, f"{TEST_TABLE}.active = %s")
        self.assertEqual(values, [1])  # MySQL uses 1 for true

    def test_build_condition_bool_false(self):
        """
        Test _build_condition_string with boolean False.

        Verifies bool is converted to 0 for MySQL.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        condition, values = adapter._build_condition_string(TEST_TABLE, 'active', False)

        self.assertEqual(condition, f"{TEST_TABLE}.active = %s")
        self.assertEqual(values, [0])  # MySQL uses 0 for false

    def test_build_condition_int(self):
        """
        Test _build_condition_string with integer value.

        Verifies int condition is created.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        condition, values = adapter._build_condition_string(TEST_TABLE, 'id', 123)

        self.assertEqual(condition, f"{TEST_TABLE}.id = %s")
        self.assertEqual(values, [123])

    def test_build_condition_list(self):
        """
        Test _build_condition_string with list value (IN clause).

        Verifies IN condition with placeholders.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        condition, values = adapter._build_condition_string(TEST_TABLE, 'status', ['active', 'pending'])

        self.assertEqual(condition, f"{TEST_TABLE}.status IN (%s, %s)")
        self.assertEqual(values, ['active', 'pending'])

    def test_build_condition_uuid(self):
        """
        Test _build_condition_string with UUID value.

        Verifies UUID is converted to string.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        test_uuid = UUID('12345678-1234-5678-1234-567812345678')
        condition, values = adapter._build_condition_string(TEST_TABLE, 'id', test_uuid)

        self.assertEqual(condition, f"{TEST_TABLE}.id = %s")
        self.assertEqual(values, [str(test_uuid)])

    def test_build_condition_none(self):
        """
        Test _build_condition_string with None value (IS NULL).

        Verifies IS NULL condition.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        condition, values = adapter._build_condition_string(TEST_TABLE, 'deleted_at', None)

        self.assertEqual(condition, f"{TEST_TABLE}.deleted_at IS NULL")
        self.assertEqual(values, [])

    def test_build_condition_unsupported_type(self):
        """
        Test _build_condition_string with unsupported type.

        Verifies TypeError is raised.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        with self.assertRaises(TypeError) as context:
            adapter._build_condition_string(TEST_TABLE, 'data', {'key': 'value'})

        self.assertIn("Unsupported type", str(context.exception))

    def test_build_condition_with_table_prefix(self):
        """
        Test _build_condition_string with column already having table prefix.

        Verifies table prefix is not added again.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        condition, values = adapter._build_condition_string(TEST_TABLE, 'other_table.name', 'value')

        self.assertEqual(condition, "other_table.name = %s")
        self.assertEqual(values, ['value'])


class TestMySqlAdapterMoveToAudit(unittest.TestCase):
    """Test move_entity_to_audit_table methods."""

    def test_get_move_entity_to_audit_table_query(self):
        """
        Test get_move_entity_to_audit_table_query returns correct query.

        Verifies query inserts from table to audit table.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        query, values = adapter.get_move_entity_to_audit_table_query(TEST_TABLE, TEST_ENTITY_ID)

        self.assertIn(f"INSERT INTO {TEST_TABLE}_audit", query)
        self.assertIn(f"SELECT * FROM {TEST_TABLE}", query)
        self.assertIn("WHERE entity_id=%s", query)
        # Entity ID should have hyphens removed
        self.assertEqual(values[0], TEST_ENTITY_ID.replace('-', ''))

    def test_move_entity_to_audit_table(self):
        """
        Test move_entity_to_audit_table executes query and commits.

        Verifies:
        - Query is executed with entity_id
        - Transaction is committed
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        mock_cursor = Mock()
        mock_connection = Mock()
        adapter._cursor = mock_cursor
        adapter._connection = mock_connection

        adapter.move_entity_to_audit_table(TEST_TABLE, TEST_ENTITY_ID)

        # Verify execute was called
        self.assertEqual(mock_cursor.execute.call_count, 1)
        call_args = mock_cursor.execute.call_args
        self.assertIn(f"{TEST_TABLE}_audit", call_args[0][0])

        # Verify commit was called
        mock_connection.commit.assert_called_once()


class TestMySqlAdapterExecuteQuery(unittest.TestCase):
    """Test execute_query method."""

    def test_execute_query_with_vars(self):
        """
        Test execute_query with query variables.

        Verifies:
        - Query is executed
        - Results are fetched
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [{'id': 1, 'name': 'test'}]
        adapter._cursor = mock_cursor

        vars_dict = {'name': 'test'}
        result = adapter.execute_query("SELECT * FROM table WHERE name=%(name)s", vars_dict)

        mock_cursor.execute.assert_called_once_with("SELECT * FROM table WHERE name=%(name)s", vars_dict)
        mock_cursor.fetchall.assert_called_once()
        self.assertEqual(result, [{'id': 1, 'name': 'test'}])

    def test_execute_query_without_vars(self):
        """
        Test execute_query without variables.

        Verifies empty dict is used as default.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = []
        adapter._cursor = mock_cursor

        adapter.execute_query("SELECT * FROM table")

        call_args = mock_cursor.execute.call_args
        self.assertEqual(call_args[0][1], {})


class TestMySqlAdapterRunTransaction(unittest.TestCase):
    """Test run_transaction method."""

    def test_run_transaction_with_tuples(self):
        """
        Test run_transaction with query tuples.

        Verifies:
        - All queries are executed
        - Transaction is committed
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        mock_cursor = Mock()
        mock_connection = Mock()
        adapter._cursor = mock_cursor
        adapter._connection = mock_connection

        queries = [
            ("INSERT INTO table1 (name) VALUES (%s)", ('test1',)),
            ("INSERT INTO table2 (name) VALUES (%s)", ('test2',))
        ]

        adapter.run_transaction(queries)

        self.assertEqual(mock_cursor.execute.call_count, 2)
        mock_connection.commit.assert_called_once()

    def test_run_transaction_with_strings(self):
        """
        Test run_transaction with plain query strings.

        Verifies queries without values work.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        mock_cursor = Mock()
        mock_connection = Mock()
        adapter._cursor = mock_cursor
        adapter._connection = mock_connection

        queries = [
            "CREATE TABLE test (id INT)",
            "CREATE INDEX idx ON test(id)"
        ]

        adapter.run_transaction(queries)

        self.assertEqual(mock_cursor.execute.call_count, 2)
        # Verify empty tuples used for queries without values
        for call in mock_cursor.execute.call_args_list:
            self.assertEqual(call[0][1], ())
        mock_connection.commit.assert_called_once()


class TestMySqlAdapterParseResponse(unittest.TestCase):
    """Test parse_db_response method."""

    def test_parse_response_list(self):
        """
        Test parsing list response.

        Verifies list is returned as-is.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        response = [{'id': 1}, {'id': 2}]
        result = adapter.parse_db_response(response)

        self.assertEqual(result, response)

    def test_parse_response_empty(self):
        """
        Test parsing empty or None response.

        Verifies empty list is returned.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        self.assertEqual(adapter.parse_db_response(None), [])
        self.assertEqual(adapter.parse_db_response([]), [])

    def test_parse_response_non_list(self):
        """
        Test parsing non-list response.

        Verifies empty list is returned.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        result = adapter.parse_db_response("not a list")

        self.assertEqual(result, [])


class TestMySqlAdapterGetOne(unittest.TestCase):
    """Test get_one method."""

    def test_get_one_basic(self):
        """
        Test get_one with basic conditions.

        Verifies:
        - SELECT query is built with WHERE active=1
        - LIMIT 1 is added
        - First result is returned
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [{'id': 1, 'name': 'test'}]
        adapter._cursor = mock_cursor

        result = adapter.get_one(TEST_TABLE, {'name': 'test'})

        # Verify query structure
        call_args = mock_cursor.execute.call_args
        query = call_args[0][0]
        self.assertIn(f"SELECT {TEST_TABLE}.*", query)
        self.assertIn(f"FROM {TEST_TABLE}", query)
        self.assertIn(f"{TEST_TABLE}.name = %s", query)
        self.assertIn(f"{TEST_TABLE}.active = %s", query)
        self.assertIn("LIMIT 1", query)

        self.assertEqual(result, {'id': 1, 'name': 'test'})

    def test_get_one_with_sort(self):
        """
        Test get_one with sort parameter.

        Verifies ORDER BY clause is added.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [{'id': 1}]
        adapter._cursor = mock_cursor

        adapter.get_one(TEST_TABLE, {}, sort=[('created_at', 'DESC')])

        call_args = mock_cursor.execute.call_args
        query = call_args[0][0]
        self.assertIn("ORDER BY created_at DESC", query)

    def test_get_one_with_joins(self):
        """
        Test get_one with join statements.

        Verifies JOIN clauses are added.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [{'id': 1}]
        adapter._cursor = mock_cursor

        join_statements = ["LEFT JOIN other_table ON test_table.id = other_table.test_id"]
        adapter.get_one(TEST_TABLE, {}, join_statements=join_statements)

        call_args = mock_cursor.execute.call_args
        query = call_args[0][0]
        self.assertIn("LEFT JOIN other_table", query)

    def test_get_one_with_additional_fields(self):
        """
        Test get_one with additional fields.

        Verifies additional fields are included in SELECT.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [{'id': 1}]
        adapter._cursor = mock_cursor

        additional_fields = ['other_table.name', 'other_table.email']
        adapter.get_one(TEST_TABLE, {}, additional_fields=additional_fields)

        call_args = mock_cursor.execute.call_args
        query = call_args[0][0]
        self.assertIn("other_table.name", query)
        self.assertIn("other_table.email", query)

    def test_get_one_not_found(self):
        """
        Test get_one when no results found.

        Verifies None is returned.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = []
        adapter._cursor = mock_cursor

        result = adapter.get_one(TEST_TABLE, {'id': 999})

        self.assertIsNone(result)


class TestMySqlAdapterGetMany(unittest.TestCase):
    """Test get_many method."""

    def test_get_many_basic(self):
        """
        Test get_many with basic conditions.

        Verifies:
        - SELECT query includes active filter by default
        - Results are returned as list
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [{'id': 1}, {'id': 2}]
        adapter._cursor = mock_cursor

        result = adapter.get_many(TEST_TABLE, {'status': 'active'})

        call_args = mock_cursor.execute.call_args
        query = call_args[0][0]
        self.assertIn(f"{TEST_TABLE}.status = %s", query)
        self.assertIn(f"{TEST_TABLE}.active = %s", query)

        self.assertEqual(len(result), 2)

    def test_get_many_with_limit_offset(self):
        """
        Test get_many with limit and offset.

        Verifies LIMIT and OFFSET clauses are added.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = []
        adapter._cursor = mock_cursor

        adapter.get_many(TEST_TABLE, {}, limit=10, offset=5)

        call_args = mock_cursor.execute.call_args
        query = call_args[0][0]
        self.assertIn("LIMIT 10", query)
        self.assertIn("OFFSET 5", query)

    def test_get_many_without_active_filter(self):
        """
        Test get_many with active=False.

        Verifies active filter is not added.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = []
        adapter._cursor = mock_cursor

        adapter.get_many(TEST_TABLE, {}, active=False)

        call_args = mock_cursor.execute.call_args
        query = call_args[0][0]
        # Should not have WHERE clause since no conditions and active=False
        self.assertNotIn("WHERE", query)

    def test_get_many_returns_dict_as_list(self):
        """
        Test get_many when response is dict instead of list.

        Verifies dict is wrapped in list.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        mock_cursor = Mock()
        # Simulate single dict response (edge case)
        adapter._cursor = mock_cursor

        # Mock parse_db_response to return dict
        with patch.object(adapter, 'parse_db_response', return_value={'id': 1}):
            with patch.object(adapter, 'execute_query', return_value=[]):
                result = adapter.get_many(TEST_TABLE, {})

        self.assertEqual(result, [{'id': 1}])


class TestMySqlAdapterGetCount(unittest.TestCase):
    """Test get_count method."""

    def test_get_count_basic(self):
        """
        Test get_count with basic conditions.

        Verifies COUNT(*) query returns count value.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [{'count': 42}]
        adapter._cursor = mock_cursor

        result = adapter.get_count(TEST_TABLE, {'status': 'active'})

        call_args = mock_cursor.execute.call_args
        query = call_args[0][0]
        self.assertIn("SELECT COUNT(*) AS `count`", query)
        self.assertIn(f"`{TEST_TABLE}`", query)

        self.assertEqual(result, 42)

    def test_get_count_empty_result(self):
        """
        Test get_count when query returns no rows.

        Verifies 0 is returned.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = []
        adapter._cursor = mock_cursor

        result = adapter.get_count(TEST_TABLE, {})

        self.assertEqual(result, 0)

    def test_get_count_with_hint_attribute_error(self):
        """
        Test get_count with hint when logger attribute doesn't exist.

        Verifies AttributeError is raised (adapter doesn't have logger).
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [{'count': 10}]
        adapter._cursor = mock_cursor

        # The adapter doesn't have a logger attribute, so this should raise AttributeError
        with self.assertRaises(AttributeError):
            adapter.get_count(TEST_TABLE, {}, options={'hint': 'some_index'})


class TestMySqlAdapterGetSaveQuery(unittest.TestCase):
    """Test get_save_query method."""

    def test_get_save_query(self):
        """
        Test get_save_query generates REPLACE INTO query.

        Verifies:
        - Uses REPLACE INTO for upsert
        - Includes all columns and placeholders
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        data = {'id': 1, 'name': 'test', 'email': 'test@example.com'}
        query, values = adapter.get_save_query(TEST_TABLE, data)

        # Verify query structure
        self.assertIn("REPLACE INTO", query)
        self.assertIn(TEST_TABLE, query)
        self.assertIn("`id`, `name`, `email`", query)
        self.assertIn("%s, %s, %s", query)

        # Verify values
        self.assertEqual(values, (1, 'test', 'test@example.com'))


class TestMySqlAdapterCreateInDatabase(unittest.TestCase):
    """Test _create_in_database method."""

    def test_create_in_database_success(self):
        """
        Test _create_in_database executes save query successfully.

        Verifies:
        - Query is executed
        - Transaction is committed
        - Returns True
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        mock_cursor = Mock()
        mock_connection = Mock()
        adapter._cursor = mock_cursor
        adapter._connection = mock_connection

        data = {'id': 1, 'name': 'test'}
        result = adapter._create_in_database(TEST_TABLE, data)

        mock_cursor.execute.assert_called_once()
        mock_connection.commit.assert_called_once()
        self.assertTrue(result)

    @patch('rococo.data.mysql.time.sleep')
    @patch('rococo.data.mysql.logging')
    def test_create_in_database_deadlock_retry(self, mock_logging, mock_sleep):
        """
        Test _create_in_database retries on deadlock.

        Verifies:
        - Deadlock error (code 1213) triggers retry
        - Exponential backoff is used
        - Success after retry returns True
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        mock_cursor = Mock()
        mock_connection = Mock()
        adapter._cursor = mock_cursor
        adapter._connection = mock_connection

        # First call raises deadlock, second succeeds
        deadlock_error = pymysql.MySQLError()
        deadlock_error.args = (1213, "Deadlock found")
        mock_cursor.execute.side_effect = [deadlock_error, None]

        data = {'id': 1}
        result = adapter._create_in_database(TEST_TABLE, data)

        # Verify retry logic
        self.assertEqual(mock_cursor.execute.call_count, 2)
        mock_connection.rollback.assert_called_once()
        mock_sleep.assert_called_once_with(1)
        mock_logging.warning.assert_called_once()
        self.assertTrue(result)

    @patch('rococo.data.mysql.logging')
    def test_create_in_database_other_error(self, mock_logging):
        """
        Test _create_in_database raises non-deadlock errors.

        Verifies error is logged and re-raised.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        mock_cursor = Mock()
        mock_connection = Mock()
        adapter._cursor = mock_cursor
        adapter._connection = mock_connection

        other_error = pymysql.MySQLError("Some other error")
        mock_cursor.execute.side_effect = other_error

        data = {'id': 1}

        with self.assertRaises(pymysql.MySQLError):
            adapter._create_in_database(TEST_TABLE, data)

        mock_connection.rollback.assert_called_once()
        mock_logging.error.assert_called_once()


class TestMySqlAdapterSave(unittest.TestCase):
    """Test save method."""

    def test_save(self):
        """
        Test save calls _create_in_database and returns data.

        Verifies data is returned after save.
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        mock_cursor = Mock()
        mock_connection = Mock()
        adapter._cursor = mock_cursor
        adapter._connection = mock_connection

        data = {'id': 1, 'name': 'test'}
        result = adapter.save(TEST_TABLE, data)

        mock_cursor.execute.assert_called_once()
        self.assertEqual(result, data)


class TestMySqlAdapterDelete(unittest.TestCase):
    """Test delete method (soft delete)."""

    def test_delete(self):
        """
        Test delete sets active=False and calls save.

        Verifies:
        - active field is set to False
        - save is called
        - Returns True
        """
        adapter = MySqlAdapter(
            host=TEST_HOST,
            port=TEST_PORT,
            user=TEST_USER,
            password=TEST_PASSWORD,
            database=TEST_DATABASE
        )

        mock_cursor = Mock()
        mock_connection = Mock()
        adapter._cursor = mock_cursor
        adapter._connection = mock_connection

        data = {'id': 1, 'name': 'test'}
        result = adapter.delete(TEST_TABLE, data)

        # Verify active was set to False
        self.assertFalse(data['active'])

        # Verify save was called (which calls _create_in_database)
        mock_cursor.execute.assert_called_once()
        self.assertTrue(result)


if __name__ == '__main__':
    unittest.main()
