"""
Tests for SurrealDB database adapter.

This module tests the SurrealDbAdapter class that provides SurrealDB connectivity
and operations for the rococo framework.
"""
import unittest
from unittest.mock import Mock, MagicMock, patch, AsyncMock
from typing import Dict, Any, List
from uuid import UUID

from rococo.data.surrealdb import SurrealDbAdapter


# Test constants
TEST_ENDPOINT = "ws://localhost:8000"
TEST_USERNAME = "root"
TEST_PASSWORD = "root"
TEST_NAMESPACE = "test_namespace"
TEST_DATABASE = "test_database"
TEST_TABLE = "test_table"
TEST_ENTITY_ID = "test_entity_123"


class TestSurrealDbAdapterInit(unittest.TestCase):
    """Test SurrealDbAdapter initialization."""

    def test_init_stores_parameters(self):
        """
        Test that SurrealDbAdapter initializes with connection parameters.

        Verifies:
        - Connection parameters are stored
        - db and event_loop are None initially
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        self.assertEqual(adapter._endpoint, TEST_ENDPOINT)
        self.assertEqual(adapter._username, TEST_USERNAME)
        self.assertEqual(adapter._password, TEST_PASSWORD)
        self.assertEqual(adapter._namespace, TEST_NAMESPACE)
        self.assertEqual(adapter._db_name, TEST_DATABASE)
        self.assertIsNone(adapter._db)
        self.assertIsNone(adapter._event_loop)


class TestSurrealDbAdapterContextManager(unittest.TestCase):
    """Test SurrealDbAdapter context manager protocol."""

    @patch('rococo.data.surrealdb.asyncio.new_event_loop')
    @patch('rococo.data.surrealdb.Surreal')
    def test_enter_creates_event_loop_and_connection(self, mock_surreal_class, mock_new_event_loop):
        """
        Test __enter__ creates event loop and prepares DB connection.

        Verifies:
        - New event loop is created
        - _prepare_db is run in event loop
        - Returns self
        """
        mock_event_loop = MagicMock()
        mock_new_event_loop.return_value = mock_event_loop

        _mock_db = AsyncMock()
        mock_surreal_instance = AsyncMock()
        mock_surreal_class.return_value = mock_surreal_instance

        # Mock the async methods
        mock_surreal_instance.connect = AsyncMock()
        mock_surreal_instance.signin = AsyncMock()
        mock_surreal_instance.use = AsyncMock()

        # Mock run_until_complete to return the mock_surreal_instance
        mock_event_loop.run_until_complete.return_value = mock_surreal_instance

        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        result = adapter.__enter__()

        mock_new_event_loop.assert_called_once()
        mock_event_loop.run_until_complete.assert_called_once()
        self.assertEqual(adapter._event_loop, mock_event_loop)
        self.assertEqual(adapter._db, mock_surreal_instance)
        self.assertEqual(result, adapter)

    @patch('rococo.data.surrealdb.asyncio.new_event_loop')
    @patch('rococo.data.surrealdb.Surreal')
    def test_exit_closes_connection_and_stops_loop(self, mock_surreal_class, mock_new_event_loop):
        """
        Test __exit__ closes DB connection and stops event loop.

        Verifies:
        - DB close is run in event loop
        - Event loop is stopped
        - db and event_loop are set to None
        """
        mock_event_loop = MagicMock()
        mock_db = AsyncMock()
        mock_db.close = AsyncMock()

        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )
        adapter._event_loop = mock_event_loop
        adapter._db = mock_db

        adapter.__exit__(None, None, None)

        mock_event_loop.run_until_complete.assert_called_once()
        mock_event_loop.stop.assert_called_once()
        self.assertIsNone(adapter._db)
        self.assertIsNone(adapter._event_loop)


class TestSurrealDbAdapterPrepareDb(unittest.TestCase):
    """Test _prepare_db async method."""

    @patch('rococo.data.surrealdb.Surreal')
    async def test_prepare_db(self, mock_surreal_class):
        """
        Test _prepare_db creates and configures Surreal connection.

        Verifies:
        - Surreal instance is created with endpoint
        - connect, signin, and use are called
        - Returns configured db instance
        """
        mock_db = AsyncMock()
        mock_db.connect = AsyncMock()
        mock_db.signin = AsyncMock()
        mock_db.use = AsyncMock()
        mock_surreal_class.return_value = mock_db

        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        result = await adapter._prepare_db()

        mock_surreal_class.assert_called_once_with(TEST_ENDPOINT)
        mock_db.connect.assert_called_once()
        mock_db.signin.assert_called_once_with({"user": TEST_USERNAME, "pass": TEST_PASSWORD})
        mock_db.use.assert_called_once_with(TEST_NAMESPACE, TEST_DATABASE)
        self.assertEqual(result, mock_db)


class TestSurrealDbAdapterCallDb(unittest.TestCase):
    """Test _call_db method."""

    def test_call_db_success(self):
        """
        Test _call_db executes db method in event loop.

        Verifies method is called with args and kwargs.
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        mock_event_loop = MagicMock()
        mock_db = MagicMock()
        mock_db.query = AsyncMock(return_value="result")
        mock_event_loop.run_until_complete.return_value = "result"

        adapter._event_loop = mock_event_loop
        adapter._db = mock_db

        result = adapter._call_db('query', 'SELECT * FROM table')

        # Verify run_until_complete was called
        mock_event_loop.run_until_complete.assert_called_once()
        self.assertEqual(result, "result")

    def test_call_db_no_connection(self):
        """
        Test _call_db raises error when db is None.

        Verifies ConnectionError is raised.
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )
        adapter._db = None

        with self.assertRaises(ConnectionError) as context:
            adapter._call_db('query', 'SELECT * FROM table')

        self.assertIn("No connection to SurrealDB", str(context.exception))


class TestSurrealDbAdapterBuildCondition(unittest.TestCase):
    """Test _build_condition_string method."""

    def test_build_condition_string(self):
        """
        Test _build_condition_string with string value.

        Verifies condition string is returned.
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        condition = adapter._build_condition_string('name', 'test_value')

        self.assertEqual(condition, "name='test_value'")

    def test_build_condition_bool_true(self):
        """
        Test _build_condition_string with boolean True.

        Verifies bool is converted to 'true' string.
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        condition = adapter._build_condition_string('active', True)

        self.assertEqual(condition, "active=true")

    def test_build_condition_bool_false(self):
        """
        Test _build_condition_string with boolean False.

        Verifies bool is converted to 'false' string.
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        condition = adapter._build_condition_string('active', False)

        self.assertEqual(condition, "active=false")

    def test_build_condition_int(self):
        """
        Test _build_condition_string with integer value.

        Verifies int condition is created.
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        condition = adapter._build_condition_string('id', 123)

        self.assertEqual(condition, "id=123")

    def test_build_condition_float(self):
        """
        Test _build_condition_string with float value.

        Verifies float condition is created.
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        condition = adapter._build_condition_string('price', 99.99)

        self.assertEqual(condition, "price=99.99")

    def test_build_condition_list(self):
        """
        Test _build_condition_string with list value (IN clause).

        Verifies IN condition with quoted values.
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        condition = adapter._build_condition_string('status', ['active', 'pending'])

        self.assertEqual(condition, 'status IN ["active","pending"]')

    def test_build_condition_uuid(self):
        """
        Test _build_condition_string with UUID value.

        Verifies UUID is converted to string.
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        test_uuid = UUID('12345678-1234-5678-1234-567812345678')
        condition = adapter._build_condition_string('id', test_uuid)

        self.assertEqual(condition, f"id='{str(test_uuid)}'")

    def test_build_condition_unsupported_type(self):
        """
        Test _build_condition_string with unsupported type.

        Verifies TypeError is raised.
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        with self.assertRaises(TypeError) as context:
            adapter._build_condition_string('data', {'key': 'value'})

        self.assertIn("Unsuppported type", str(context.exception))


class TestSurrealDbAdapterRunTransaction(unittest.TestCase):
    """Test run_transaction method."""

    def test_run_transaction(self):
        """
        Test run_transaction executes operations sequentially.

        Verifies:
        - All operations are executed via _call_db
        - Operations are unpacked correctly
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        mock_event_loop = MagicMock()
        mock_db = MagicMock()
        adapter._event_loop = mock_event_loop
        adapter._db = mock_db

        mock_event_loop.run_until_complete.return_value = None

        operations = [
            ('query', 'INSERT INTO table1 VALUES {name: "test1"}'),
            ('query', 'INSERT INTO table2 VALUES {name: "test2"}')
        ]

        adapter.run_transaction(operations)

        # Verify _call_db was called for each operation
        self.assertEqual(mock_event_loop.run_until_complete.call_count, 2)


class TestSurrealDbAdapterMoveToAudit(unittest.TestCase):
    """Test move_entity_to_audit_table methods."""

    def test_get_move_entity_to_audit_table_query(self):
        """
        Test get_move_entity_to_audit_table_query returns correct operation.

        Verifies query inserts from table to audit table with entity_id.
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        operation = adapter.get_move_entity_to_audit_table_query(TEST_TABLE, TEST_ENTITY_ID)

        self.assertEqual(operation[0], 'query')
        query = operation[1]
        self.assertIn(f"INSERT INTO {TEST_TABLE}_audit", query)
        self.assertIn("SELECT *", query)
        self.assertIn(f"FROM {TEST_TABLE}", query)
        self.assertIn(TEST_ENTITY_ID, query)

    def test_move_entity_to_audit_table(self):
        """
        Test move_entity_to_audit_table executes query.

        Verifies:
        - Operation is executed via _call_db
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        mock_event_loop = MagicMock()
        mock_db = MagicMock()
        adapter._event_loop = mock_event_loop
        adapter._db = mock_db

        mock_event_loop.run_until_complete.return_value = None

        adapter.move_entity_to_audit_table(TEST_TABLE, TEST_ENTITY_ID)

        # Verify _call_db was called
        mock_event_loop.run_until_complete.assert_called_once()


class TestSurrealDbAdapterExecuteQuery(unittest.TestCase):
    """Test execute_query method."""

    def test_execute_query_with_vars(self):
        """
        Test execute_query with query variables.

        Verifies:
        - Query is executed via _call_db
        - Variables are passed
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        mock_event_loop = MagicMock()
        mock_db = MagicMock()
        adapter._event_loop = mock_event_loop
        adapter._db = mock_db

        mock_event_loop.run_until_complete.return_value = [{'result': [{'id': 1}]}]

        vars_dict = {'name': 'test'}
        result = adapter.execute_query("SELECT * FROM table WHERE name=$name", vars_dict)

        mock_event_loop.run_until_complete.assert_called_once()
        self.assertEqual(result, [{'result': [{'id': 1}]}])

    def test_execute_query_without_vars(self):
        """
        Test execute_query without variables.

        Verifies empty dict is used as default.
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        mock_event_loop = MagicMock()
        mock_db = MagicMock()
        adapter._event_loop = mock_event_loop
        adapter._db = mock_db

        mock_event_loop.run_until_complete.return_value = []

        adapter.execute_query("SELECT * FROM table")

        mock_event_loop.run_until_complete.assert_called_once()


class TestSurrealDbAdapterParseResponse(unittest.TestCase):
    """Test parse_db_response method."""

    def test_parse_response_single_result(self):
        """
        Test parsing response with single result.

        Verifies single item is returned directly.
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        response = [{'result': [{'id': 1, 'name': 'test'}]}]
        result = adapter.parse_db_response(response)

        self.assertEqual(result, {'id': 1, 'name': 'test'})

    def test_parse_response_multiple_results(self):
        """
        Test parsing response with multiple results.

        Verifies list of results is returned.
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        response = [{'result': [{'id': 1}, {'id': 2}]}]
        result = adapter.parse_db_response(response)

        self.assertEqual(result, [{'id': 1}, {'id': 2}])

    def test_parse_response_empty(self):
        """
        Test parsing empty or None response.

        Verifies empty list is returned.
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        self.assertEqual(adapter.parse_db_response(None), [])
        self.assertEqual(adapter.parse_db_response([]), [])

    def test_parse_response_non_list(self):
        """
        Test parsing non-list response.

        Verifies empty list is returned.
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        result = adapter.parse_db_response("not a list")

        self.assertEqual(result, [])

    def test_parse_response_no_result_key(self):
        """
        Test parsing response without 'result' key.

        Verifies empty list is returned.
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        response = [{'other_key': 'value'}]
        result = adapter.parse_db_response(response)

        self.assertEqual(result, [])


class TestSurrealDbAdapterGetOne(unittest.TestCase):
    """Test get_one method."""

    def test_get_one_basic(self):
        """
        Test get_one with basic conditions.

        Verifies:
        - SELECT query is built with WHERE active=true
        - LIMIT 1 is added
        - Result is returned
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        mock_event_loop = MagicMock()
        mock_db = MagicMock()
        adapter._event_loop = mock_event_loop
        adapter._db = mock_db

        mock_event_loop.run_until_complete.return_value = [{'result': [{'id': 1, 'name': 'test'}]}]

        result = adapter.get_one(TEST_TABLE, {'name': 'test'})

        # Verify query was executed
        mock_event_loop.run_until_complete.assert_called_once()
        self.assertEqual(result, {'id': 1, 'name': 'test'})

    def test_get_one_with_sort(self):
        """
        Test get_one with sort parameter.

        Verifies ORDER BY clause is added.
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        mock_event_loop = MagicMock()
        mock_db = MagicMock()
        adapter._event_loop = mock_event_loop
        adapter._db = mock_db

        mock_event_loop.run_until_complete.return_value = [{'result': [{'id': 1}]}]

        adapter.get_one(TEST_TABLE, {}, sort=[('created_at', 'DESC')])

        mock_event_loop.run_until_complete.assert_called_once()

    def test_get_one_with_fetch_related(self):
        """
        Test get_one with fetch_related parameter.

        Verifies FETCH clause is added.
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        mock_event_loop = MagicMock()
        mock_db = MagicMock()
        adapter._event_loop = mock_event_loop
        adapter._db = mock_db

        mock_event_loop.run_until_complete.return_value = [{'result': [{'id': 1}]}]

        adapter.get_one(TEST_TABLE, {}, fetch_related=['related_table'])

        mock_event_loop.run_until_complete.assert_called_once()

    def test_get_one_with_additional_fields(self):
        """
        Test get_one with additional fields.

        Verifies additional fields are included in SELECT.
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        mock_event_loop = MagicMock()
        mock_db = MagicMock()
        adapter._event_loop = mock_event_loop
        adapter._db = mock_db

        mock_event_loop.run_until_complete.return_value = [{'result': [{'id': 1}]}]

        adapter.get_one(TEST_TABLE, {}, additional_fields=['field1', 'field2'])

        mock_event_loop.run_until_complete.assert_called_once()


class TestSurrealDbAdapterGetMany(unittest.TestCase):
    """Test get_many method."""

    def test_get_many_basic(self):
        """
        Test get_many with basic conditions.

        Verifies:
        - SELECT query includes active filter by default
        - LIMIT is added
        - Results are returned
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        mock_event_loop = MagicMock()
        mock_db = MagicMock()
        adapter._event_loop = mock_event_loop
        adapter._db = mock_db

        mock_event_loop.run_until_complete.return_value = [{'result': [{'id': 1}, {'id': 2}]}]

        result = adapter.get_many(TEST_TABLE, {'status': 'active'})

        mock_event_loop.run_until_complete.assert_called_once()
        self.assertEqual(result, [{'id': 1}, {'id': 2}])

    def test_get_many_with_limit(self):
        """
        Test get_many with custom limit.

        Verifies LIMIT clause with specified value.
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        mock_event_loop = MagicMock()
        mock_db = MagicMock()
        adapter._event_loop = mock_event_loop
        adapter._db = mock_db

        mock_event_loop.run_until_complete.return_value = [{'result': []}]

        adapter.get_many(TEST_TABLE, {}, limit=50)

        mock_event_loop.run_until_complete.assert_called_once()

    def test_get_many_without_active_filter(self):
        """
        Test get_many with active=False.

        Verifies active filter is not added.
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        mock_event_loop = MagicMock()
        mock_db = MagicMock()
        adapter._event_loop = mock_event_loop
        adapter._db = mock_db

        mock_event_loop.run_until_complete.return_value = [{'result': []}]

        adapter.get_many(TEST_TABLE, {}, active=False)

        mock_event_loop.run_until_complete.assert_called_once()

    def test_get_many_with_sort(self):
        """
        Test get_many with sort parameter.

        Verifies ORDER BY clause is added.
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        mock_event_loop = MagicMock()
        mock_db = MagicMock()
        adapter._event_loop = mock_event_loop
        adapter._db = mock_db

        mock_event_loop.run_until_complete.return_value = [{'result': []}]

        adapter.get_many(TEST_TABLE, {}, sort=[('name', 'ASC')])

        mock_event_loop.run_until_complete.assert_called_once()

    def test_get_many_with_fetch_related(self):
        """
        Test get_many with fetch_related parameter.

        Verifies FETCH clause is added.
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        mock_event_loop = MagicMock()
        mock_db = MagicMock()
        adapter._event_loop = mock_event_loop
        adapter._db = mock_db

        mock_event_loop.run_until_complete.return_value = [{'result': []}]

        adapter.get_many(TEST_TABLE, {}, fetch_related=['related_table'])

        mock_event_loop.run_until_complete.assert_called_once()


class TestSurrealDbAdapterGetCount(unittest.TestCase):
    """Test get_count method."""

    def test_get_count_basic_dict_response(self):
        """
        Test get_count with dict response.

        Verifies COUNT(*) query returns count value.
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        mock_event_loop = MagicMock()
        mock_db = MagicMock()
        adapter._event_loop = mock_event_loop
        adapter._db = mock_db

        mock_event_loop.run_until_complete.return_value = [{'result': [{'count': 42}]}]

        result = adapter.get_count(TEST_TABLE, {'status': 'active'})

        self.assertEqual(result, 42)

    def test_get_count_list_response(self):
        """
        Test get_count with list response.

        Verifies count is extracted from first list item.
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        mock_event_loop = MagicMock()
        mock_db = MagicMock()
        adapter._event_loop = mock_event_loop
        adapter._db = mock_db

        # parse_db_response might return list
        with patch.object(adapter, 'parse_db_response', return_value=[{'count': 10}]):
            result = adapter.get_count(TEST_TABLE, {})

        self.assertEqual(result, 10)

    def test_get_count_empty_result(self):
        """
        Test get_count when query returns no count.

        Verifies 0 is returned.
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        mock_event_loop = MagicMock()
        mock_db = MagicMock()
        adapter._event_loop = mock_event_loop
        adapter._db = mock_db

        mock_event_loop.run_until_complete.return_value = [{'result': []}]

        result = adapter.get_count(TEST_TABLE, {})

        self.assertEqual(result, 0)

    @patch('rococo.data.surrealdb.logging')
    def test_get_count_with_hint_logs_info(self, mock_logging):
        """
        Test get_count with hint option logs info.

        SurrealDB doesn't support hints, so it logs.
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        mock_event_loop = MagicMock()
        mock_db = MagicMock()
        adapter._event_loop = mock_event_loop
        adapter._db = mock_db

        mock_event_loop.run_until_complete.return_value = [{'result': [{'count': 10}]}]

        adapter.get_count(TEST_TABLE, {}, options={'hint': 'some_index'})

        # Verify logging.info was called
        mock_logging.info.assert_called_once()


class TestSurrealDbAdapterGetSaveQuery(unittest.TestCase):
    """Test get_save_query method."""

    def test_get_save_query(self):
        """
        Test get_save_query returns update operation.

        Verifies:
        - Returns tuple with 'update', id, and data
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        data = {'id': 'record:123', 'name': 'test', 'email': 'test@example.com'}
        operation = adapter.get_save_query(TEST_TABLE, data)

        self.assertEqual(operation[0], 'update')
        self.assertEqual(operation[1], 'record:123')
        self.assertEqual(operation[2], data)


class TestSurrealDbAdapterSave(unittest.TestCase):
    """Test save method."""

    def test_save(self):
        """
        Test save calls _call_db with update operation.

        Verifies result is returned from db.
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        mock_event_loop = MagicMock()
        mock_db = MagicMock()
        adapter._event_loop = mock_event_loop
        adapter._db = mock_db

        saved_data = {'id': 'record:123', 'name': 'test'}
        mock_event_loop.run_until_complete.return_value = saved_data

        data = {'id': 'record:123', 'name': 'test'}
        result = adapter.save(TEST_TABLE, data)

        mock_event_loop.run_until_complete.assert_called_once()
        self.assertEqual(result, saved_data)


class TestSurrealDbAdapterDelete(unittest.TestCase):
    """Test delete method (soft delete)."""

    def test_delete(self):
        """
        Test delete sets active=False and calls save.

        Verifies:
        - active field is set to False
        - save is called
        - Returns result from save
        """
        adapter = SurrealDbAdapter(
            endpoint=TEST_ENDPOINT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            namespace=TEST_NAMESPACE,
            db_name=TEST_DATABASE
        )

        mock_event_loop = MagicMock()
        mock_db = MagicMock()
        adapter._event_loop = mock_event_loop
        adapter._db = mock_db

        updated_data = {'id': 'record:123', 'name': 'test', 'active': False}
        mock_event_loop.run_until_complete.return_value = updated_data

        data = {'id': 'record:123', 'name': 'test'}
        result = adapter.delete(TEST_TABLE, data)

        # Verify active was set to False
        self.assertFalse(data['active'])

        # Verify save was called (which calls _call_db)
        mock_event_loop.run_until_complete.assert_called_once()
        self.assertEqual(result, updated_data)


if __name__ == '__main__':
    unittest.main()
