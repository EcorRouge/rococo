"""
Tests for MongoDB database adapter.

This module tests the MongoDBAdapter class that provides MongoDB connectivity
and operations for the rococo framework.
"""
import unittest
from unittest.mock import Mock, MagicMock, patch, call
from typing import Dict, Any, List

from pymongo import errors
from rococo.data.mongodb import MongoDBAdapter


# Test constants
TEST_URI = "mongodb://localhost:27017"
TEST_DATABASE = "test_database"
TEST_COLLECTION = "test_collection"
TEST_ENTITY_ID = "test_entity_123"


class TestMongoDBAdapterInit(unittest.TestCase):
    """Test MongoDBAdapter initialization."""

    @patch('rococo.data.mongodb.MongoClient')
    def test_init_with_default_options(self, mock_mongo_client):
        """
        Test that MongoDBAdapter initializes MongoClient with default options.

        Verifies:
        - MongoClient is called with uri and default options
        - Database name is stored
        - Session is initialized to None
        """
        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)

        expected_options = {
            'retryWrites': True,
            'w': 'majority',
            'serverSelectionTimeoutMS': 5000,
            'connectTimeoutMS': 5000,
            'maxPoolSize': 100,
            'tz_aware': True,
        }
        mock_mongo_client.assert_called_once_with(TEST_URI, **expected_options)
        self.assertEqual(adapter.db_name, TEST_DATABASE)
        self.assertIsNone(adapter._session)

    @patch('rococo.data.mongodb.MongoClient')
    def test_init_with_custom_options(self, mock_mongo_client):
        """
        Test that custom client options override defaults.

        Verifies custom options are merged with defaults.
        """
        custom_options = {'maxPoolSize': 200, 'retryWrites': False}
        MongoDBAdapter(TEST_URI, TEST_DATABASE, **custom_options)

        # Custom options should override defaults
        call_args = mock_mongo_client.call_args
        self.assertEqual(call_args[1]['maxPoolSize'], 200)
        self.assertFalse(call_args[1]['retryWrites'])
        self.assertEqual(call_args[1]['w'], 'majority')  # Default preserved


class TestMongoDBAdapterContextManager(unittest.TestCase):
    """Test MongoDBAdapter context manager protocol."""

    @patch('rococo.data.mongodb.MongoClient')
    def test_enter_success(self, mock_mongo_client):
        """
        Test successful context manager entry.

        Verifies:
        - Ping command is executed
        - Database is selected
        - Session is started with causal consistency
        - Returns self
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        # Mock admin.command('ping') to succeed
        mock_client_instance.admin.command.return_value = {'ok': 1}

        # Mock database and session
        mock_db = Mock()
        mock_client_instance.get_database.return_value = mock_db
        mock_session = Mock()
        mock_client_instance.start_session.return_value = mock_session

        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        result = adapter.__enter__()

        # Assert
        mock_client_instance.admin.command.assert_called_once_with('ping')
        mock_client_instance.get_database.assert_called_once_with(TEST_DATABASE)
        mock_client_instance.start_session.assert_called_once_with(causal_consistency=True)
        self.assertEqual(adapter.db, mock_db)
        self.assertEqual(adapter._session, mock_session)
        self.assertEqual(result, adapter)

    @patch('rococo.data.mongodb.MongoClient')
    def test_enter_connection_failure(self, mock_mongo_client):
        """
        Test context manager entry with connection failure.

        Verifies ConnectionError is raised when ping fails.
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        # Mock ping to raise PyMongoError
        mock_client_instance.admin.command.side_effect = errors.PyMongoError("Connection failed")

        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)

        with self.assertRaises(ConnectionError) as context:
            adapter.__enter__()

        self.assertIn("MongoDB ping failed", str(context.exception))

    @patch('rococo.data.mongodb.MongoClient')
    def test_exit_cleans_up_session(self, mock_mongo_client):
        """
        Test that __exit__ cleans up the session.

        Verifies:
        - Session is ended if it exists
        - Session is set to None
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        mock_session = Mock()
        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter._session = mock_session

        adapter.__exit__(None, None, None)

        mock_session.end_session.assert_called_once()
        self.assertIsNone(adapter._session)

    @patch('rococo.data.mongodb.MongoClient')
    def test_exit_without_session(self, mock_mongo_client):
        """
        Test __exit__ when session is None.

        Verifies no error is raised.
        """
        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter._session = None

        # Should not raise
        adapter.__exit__(None, None, None)


class TestMongoDBAdapterGetCollection(unittest.TestCase):
    """Test _get_collection method."""

    @patch('rococo.data.mongodb.MongoClient')
    def test_get_collection_read_only(self, mock_mongo_client):
        """
        Test _get_collection for read operations.

        Verifies local read concern without write concern.
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance
        mock_db = Mock()

        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter.db = mock_db

        adapter._get_collection(TEST_COLLECTION, write=False)

        # Verify get_collection called with read_concern only
        call_args = mock_db.get_collection.call_args
        self.assertEqual(call_args[0][0], TEST_COLLECTION)
        self.assertIn('read_concern', call_args[1])
        self.assertNotIn('write_concern', call_args[1])

    @patch('rococo.data.mongodb.MongoClient')
    def test_get_collection_with_write(self, mock_mongo_client):
        """
        Test _get_collection for write operations.

        Verifies both read and write concerns are set.
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance
        mock_db = Mock()

        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter.db = mock_db

        adapter._get_collection(TEST_COLLECTION, write=True)

        # Verify get_collection called with both concerns
        call_args = mock_db.get_collection.call_args
        self.assertEqual(call_args[0][0], TEST_COLLECTION)
        self.assertIn('read_concern', call_args[1])
        self.assertIn('write_concern', call_args[1])


class TestMongoDBAdapterTransactions(unittest.TestCase):
    """Test transaction handling."""

    @patch('rococo.data.mongodb.MongoClient')
    def test_run_transaction_success(self, mock_mongo_client):
        """
        Test successful transaction execution.

        Verifies:
        - All operations are executed within transaction
        - Transaction is started on session
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        mock_session = Mock()
        mock_transaction = MagicMock()
        mock_session.start_transaction.return_value = mock_transaction
        mock_transaction.__enter__ = Mock(return_value=mock_transaction)
        mock_transaction.__exit__ = Mock(return_value=False)

        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter._session = mock_session

        # Create mock operations
        op1 = Mock()
        op2 = Mock()
        operations = [op1, op2]

        adapter.run_transaction(operations)

        # Verify
        mock_session.start_transaction.assert_called_once()
        op1.assert_called_once()
        op2.assert_called_once()

    @patch('rococo.data.mongodb.MongoClient')
    def test_run_transaction_without_session(self, mock_mongo_client):
        """
        Test run_transaction raises error when session not started.

        Verifies RuntimeError is raised.
        """
        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter._session = None

        with self.assertRaises(RuntimeError) as context:
            adapter.run_transaction([])

        self.assertIn("Session not started", str(context.exception))


class TestMongoDBAdapterExecuteQuery(unittest.TestCase):
    """Test execute_query method."""

    @patch('rococo.data.mongodb.MongoClient')
    def test_execute_query_raises_not_implemented(self, mock_mongo_client):
        """
        Test that execute_query raises NotImplementedError.

        MongoDB uses different query language, so SQL queries not supported.
        """
        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)

        with self.assertRaises(NotImplementedError) as context:
            adapter.execute_query("SELECT * FROM table")

        self.assertIn("not supported for MongoDB", str(context.exception))


class TestMongoDBAdapterParseResponse(unittest.TestCase):
    """Test parse_db_response method."""

    @patch('rococo.data.mongodb.MongoClient')
    def test_parse_response_dict(self, mock_mongo_client):
        """
        Test parsing a single document response.

        Verifies dict is returned as-is.
        """
        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        doc = {'_id': '123', 'name': 'test'}

        result = adapter.parse_db_response(doc)

        self.assertEqual(result, doc)

    @patch('rococo.data.mongodb.MongoClient')
    def test_parse_response_cursor(self, mock_mongo_client):
        """
        Test parsing a cursor response.

        Verifies cursor is converted to list.
        """
        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)

        # Mock cursor
        doc1 = {'_id': '1', 'name': 'doc1'}
        doc2 = {'_id': '2', 'name': 'doc2'}
        cursor = iter([doc1, doc2])

        result = adapter.parse_db_response(cursor)

        self.assertEqual(result, [doc1, doc2])


class TestMongoDBAdapterGetOne(unittest.TestCase):
    """Test get_one method."""

    @patch('rococo.data.mongodb.MongoClient')
    def test_get_one_success(self, mock_mongo_client):
        """
        Test successful get_one query.

        Verifies find_one is called with conditions.
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        mock_db = Mock()
        mock_collection = Mock()
        mock_db.get_collection.return_value = mock_collection

        expected_doc = {'_id': '123', 'name': 'test'}
        mock_collection.find_one.return_value = expected_doc

        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter.db = mock_db

        conditions = {'name': 'test'}
        result = adapter.get_one(TEST_COLLECTION, conditions)

        mock_collection.find_one.assert_called_once_with(conditions)
        self.assertEqual(result, expected_doc)

    @patch('rococo.data.mongodb.MongoClient')
    def test_get_one_with_hint_and_sort(self, mock_mongo_client):
        """
        Test get_one with hint and sort options.

        Verifies options are passed to find_one.
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        mock_db = Mock()
        mock_collection = Mock()
        mock_db.get_collection.return_value = mock_collection
        mock_collection.find_one.return_value = {'_id': '123'}

        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter.db = mock_db

        conditions = {'name': 'test'}
        hint = 'name_index'
        sort = [('name', 1)]

        adapter.get_one(TEST_COLLECTION, conditions, hint=hint, sort=sort)

        call_args = mock_collection.find_one.call_args
        self.assertEqual(call_args[0][0], conditions)
        self.assertEqual(call_args[1]['hint'], hint)
        self.assertEqual(call_args[1]['sort'], sort)

    @patch('rococo.data.mongodb.MongoClient')
    def test_get_one_error(self, mock_mongo_client):
        """
        Test get_one with PyMongoError.

        Verifies RuntimeError is raised.
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        mock_db = Mock()
        mock_collection = Mock()
        mock_db.get_collection.return_value = mock_collection
        mock_collection.find_one.side_effect = errors.PyMongoError("Query failed")

        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter.db = mock_db

        with self.assertRaises(RuntimeError) as context:
            adapter.get_one(TEST_COLLECTION, {'name': 'test'})

        self.assertIn("get_one failed", str(context.exception))


class TestMongoDBAdapterGetMany(unittest.TestCase):
    """Test get_many method."""

    @patch('rococo.data.mongodb.MongoClient')
    def test_get_many_basic(self, mock_mongo_client):
        """
        Test get_many with basic conditions.

        Verifies find is called and results are returned as list.
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        mock_db = Mock()
        mock_collection = Mock()
        mock_db.get_collection.return_value = mock_collection

        docs = [{'_id': '1'}, {'_id': '2'}]
        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(return_value=iter(docs))
        mock_collection.find.return_value = mock_cursor

        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter.db = mock_db

        conditions = {'active': True}
        result = adapter.get_many(TEST_COLLECTION, conditions)

        mock_collection.find.assert_called_once()
        self.assertEqual(result, docs)

    @patch('rococo.data.mongodb.MongoClient')
    def test_get_many_with_sort_limit_offset(self, mock_mongo_client):
        """
        Test get_many with sort, limit, and offset.

        Verifies cursor methods are called in correct order.
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        mock_db = Mock()
        mock_collection = Mock()
        mock_db.get_collection.return_value = mock_collection

        # Mock cursor with chaining
        mock_cursor = MagicMock()
        docs = [{'_id': '1'}]
        mock_cursor.__iter__ = Mock(return_value=iter(docs))
        mock_cursor.sort.return_value = mock_cursor
        mock_cursor.skip.return_value = mock_cursor
        mock_cursor.limit.return_value = mock_cursor
        mock_collection.find.return_value = mock_cursor

        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter.db = mock_db

        sort = [('name', 1)]
        result = adapter.get_many(
            TEST_COLLECTION,
            {'active': True},
            sort=sort,
            limit=10,
            offset=5
        )

        mock_cursor.sort.assert_called_once_with(sort)
        mock_cursor.skip.assert_called_once_with(5)
        mock_cursor.limit.assert_called_once_with(10)
        self.assertEqual(result, docs)

    @patch('rococo.data.mongodb.MongoClient')
    def test_get_many_with_hint(self, mock_mongo_client):
        """
        Test get_many with index hint.

        Verifies hint is passed to find.
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        mock_db = Mock()
        mock_collection = Mock()
        mock_db.get_collection.return_value = mock_collection

        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(return_value=iter([]))
        mock_collection.find.return_value = mock_cursor

        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter.db = mock_db

        adapter.get_many(TEST_COLLECTION, {}, hint='name_index')

        call_args = mock_collection.find.call_args
        self.assertEqual(call_args[1]['hint'], 'name_index')

    @patch('rococo.data.mongodb.MongoClient')
    def test_get_many_error(self, mock_mongo_client):
        """
        Test get_many with PyMongoError.

        Verifies RuntimeError is raised.
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        mock_db = Mock()
        mock_collection = Mock()
        mock_db.get_collection.return_value = mock_collection
        mock_collection.find.side_effect = errors.PyMongoError("Query failed")

        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter.db = mock_db

        with self.assertRaises(RuntimeError) as context:
            adapter.get_many(TEST_COLLECTION, {})

        self.assertIn("get_many failed", str(context.exception))


class TestMongoDBAdapterGetCount(unittest.TestCase):
    """Test get_count method."""

    @patch('rococo.data.mongodb.MongoClient')
    def test_get_count_basic(self, mock_mongo_client):
        """
        Test basic get_count query.

        Verifies count_documents is called with conditions.
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        mock_db = Mock()
        mock_collection = Mock()
        mock_db.get_collection.return_value = mock_collection
        mock_collection.count_documents.return_value = 42

        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter.db = mock_db

        conditions = {'active': True}
        result = adapter.get_count(TEST_COLLECTION, conditions)

        mock_collection.count_documents.assert_called_once_with(conditions)
        self.assertEqual(result, 42)

    @patch('rococo.data.mongodb.MongoClient')
    def test_get_count_with_hint(self, mock_mongo_client):
        """
        Test get_count with hint option.

        Verifies hint is forwarded to count_documents.
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        mock_db = Mock()
        mock_collection = Mock()
        mock_db.get_collection.return_value = mock_collection
        mock_collection.count_documents.return_value = 10

        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter.db = mock_db

        options = {'hint': 'name_index'}
        adapter.get_count(TEST_COLLECTION, {}, options=options)

        call_args = mock_collection.count_documents.call_args
        self.assertEqual(call_args[1]['hint'], 'name_index')

    @patch('rococo.data.mongodb.MongoClient')
    def test_get_count_error(self, mock_mongo_client):
        """
        Test get_count with PyMongoError.

        Verifies RuntimeError is raised.
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        mock_db = Mock()
        mock_collection = Mock()
        mock_db.get_collection.return_value = mock_collection
        mock_collection.count_documents.side_effect = errors.PyMongoError("Count failed")

        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter.db = mock_db

        with self.assertRaises(RuntimeError) as context:
            adapter.get_count(TEST_COLLECTION, {})

        self.assertIn("get_count failed", str(context.exception))


class TestMongoDBAdapterMoveToAudit(unittest.TestCase):
    """Test move_entity_to_audit_table method."""

    @patch('rococo.data.mongodb.MongoClient')
    def test_move_to_audit_success(self, mock_mongo_client):
        """
        Test successful move to audit table.

        Verifies:
        - All documents with entity_id are found
        - Documents are copied to audit collection with replace_one upsert
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        mock_db = Mock()
        mock_source_collection = Mock()
        mock_audit_collection = Mock()

        def get_collection_side_effect(name, **kwargs):
            if name == TEST_COLLECTION:
                return mock_source_collection
            elif name == f"{TEST_COLLECTION}_audit":
                return mock_audit_collection
            return Mock()

        mock_db.get_collection.side_effect = get_collection_side_effect

        docs = [
            {'_id': 'doc1', 'entity_id': TEST_ENTITY_ID, 'version': 1},
            {'_id': 'doc2', 'entity_id': TEST_ENTITY_ID, 'version': 2}
        ]
        mock_source_collection.find.return_value = docs

        mock_session = Mock()
        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter.db = mock_db
        adapter._session = mock_session

        adapter.move_entity_to_audit_table(TEST_COLLECTION, TEST_ENTITY_ID)

        # Verify find was called with entity_id
        mock_source_collection.find.assert_called_once_with(
            {'entity_id': TEST_ENTITY_ID},
            session=mock_session
        )

        # Verify replace_one called for each document
        self.assertEqual(mock_audit_collection.replace_one.call_count, 2)
        calls = mock_audit_collection.replace_one.call_args_list
        for i, doc in enumerate(docs):
            self.assertEqual(calls[i][0][0], {'_id': doc['_id']})
            self.assertEqual(calls[i][0][1], doc)
            self.assertTrue(calls[i][1]['upsert'])
            self.assertEqual(calls[i][1]['session'], mock_session)

    @patch('rococo.data.mongodb.MongoClient')
    def test_move_to_audit_no_documents(self, mock_mongo_client):
        """
        Test move_to_audit_table when no documents exist.

        Verifies no audit operations are performed.
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        mock_db = Mock()
        mock_source_collection = Mock()
        mock_db.get_collection.return_value = mock_source_collection
        mock_source_collection.find.return_value = []

        mock_session = Mock()
        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter.db = mock_db
        adapter._session = mock_session

        # Should not raise
        adapter.move_entity_to_audit_table(TEST_COLLECTION, TEST_ENTITY_ID)

    @patch('rococo.data.mongodb.MongoClient')
    def test_move_to_audit_error(self, mock_mongo_client):
        """
        Test move_to_audit_table with PyMongoError.

        Verifies RuntimeError is raised.
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        mock_db = Mock()
        mock_collection = Mock()
        mock_db.get_collection.return_value = mock_collection
        mock_collection.find.side_effect = errors.PyMongoError("Find failed")

        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter.db = mock_db
        adapter._session = Mock()

        with self.assertRaises(RuntimeError) as context:
            adapter.move_entity_to_audit_table(TEST_COLLECTION, TEST_ENTITY_ID)

        self.assertIn("move_entity_to_audit_table failed", str(context.exception))


class TestMongoDBAdapterSave(unittest.TestCase):
    """Test save method (versioned insert)."""

    @patch('rococo.data.mongodb.MongoClient')
    def test_save_new_entity(self, mock_mongo_client):
        """
        Test saving a new entity (no previous version).

        Verifies:
        - find_one returns None (no previous version)
        - insert_one is called with latest=True
        - Inserted document is returned
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        mock_db = Mock()
        mock_collection = Mock()
        mock_db.get_collection.return_value = mock_collection

        # No previous version
        mock_collection.find_one.side_effect = [None, {'_id': 'new_id', 'entity_id': TEST_ENTITY_ID, 'latest': True}]

        mock_insert_result = Mock()
        mock_insert_result.inserted_id = 'new_id'
        mock_collection.insert_one.return_value = mock_insert_result

        mock_session = Mock()
        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter.db = mock_db
        adapter._session = mock_session

        data = {'entity_id': TEST_ENTITY_ID, 'name': 'test'}
        result = adapter.save(TEST_COLLECTION, data)

        # Verify no update_one called (no previous version)
        mock_collection.update_one.assert_not_called()

        # Verify insert_one called with latest=True
        insert_call_args = mock_collection.insert_one.call_args
        inserted_data = insert_call_args[0][0]
        self.assertEqual(inserted_data['entity_id'], TEST_ENTITY_ID)
        self.assertTrue(inserted_data['latest'])
        self.assertEqual(insert_call_args[1]['session'], mock_session)

        # Verify result
        self.assertEqual(result['_id'], 'new_id')
        self.assertTrue(result['latest'])

    @patch('rococo.data.mongodb.MongoClient')
    def test_save_update_existing(self, mock_mongo_client):
        """
        Test saving a new version of existing entity.

        Verifies:
        - Previous version is marked latest=False
        - New version is inserted with latest=True
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        mock_db = Mock()
        mock_collection = Mock()
        mock_db.get_collection.return_value = mock_collection

        # Mock previous version
        prev_doc = {'_id': 'old_id', 'entity_id': TEST_ENTITY_ID, 'latest': True}
        new_doc = {'_id': 'new_id', 'entity_id': TEST_ENTITY_ID, 'latest': True}

        mock_collection.find_one.side_effect = [prev_doc, new_doc]

        mock_insert_result = Mock()
        mock_insert_result.inserted_id = 'new_id'
        mock_collection.insert_one.return_value = mock_insert_result

        mock_session = Mock()
        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter.db = mock_db
        adapter._session = mock_session

        data = {'entity_id': TEST_ENTITY_ID, 'name': 'updated'}
        result = adapter.save(TEST_COLLECTION, data)

        # Verify update_one called to mark previous as latest=False
        update_call_args = mock_collection.update_one.call_args_list[0]
        self.assertEqual(update_call_args[0][0], {'_id': 'old_id'})
        self.assertEqual(update_call_args[0][1], {'$set': {'latest': False}})
        self.assertEqual(update_call_args[1]['session'], mock_session)

        # Verify insert_one called
        self.assertEqual(mock_collection.insert_one.call_count, 1)

        # Verify result
        self.assertEqual(result['_id'], 'new_id')
        self.assertTrue(result['latest'])

    @patch('rococo.data.mongodb.MongoClient')
    def test_save_missing_entity_id(self, mock_mongo_client):
        """
        Test save raises error when entity_id missing.

        Verifies RuntimeError is raised.
        """
        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)

        data = {'name': 'test'}  # Missing entity_id

        with self.assertRaises(RuntimeError) as context:
            adapter.save(TEST_COLLECTION, data)

        self.assertIn("entity_id' is required", str(context.exception))

    @patch('rococo.data.mongodb.MongoClient')
    def test_save_error(self, mock_mongo_client):
        """
        Test save with PyMongoError.

        Verifies RuntimeError is raised.
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        mock_db = Mock()
        mock_collection = Mock()
        mock_db.get_collection.return_value = mock_collection
        mock_collection.find_one.side_effect = errors.PyMongoError("Find failed")

        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter.db = mock_db
        adapter._session = Mock()

        data = {'entity_id': TEST_ENTITY_ID}

        with self.assertRaises(RuntimeError) as context:
            adapter.save(TEST_COLLECTION, data)

        self.assertIn("save failed", str(context.exception))


class TestMongoDBAdapterDelete(unittest.TestCase):
    """Test delete method (soft delete)."""

    @patch('rococo.data.mongodb.MongoClient')
    def test_delete_success(self, mock_mongo_client):
        """
        Test successful soft delete.

        Verifies:
        - update_one is called with active=False
        - Returns True when document is modified
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        mock_db = Mock()
        mock_collection = Mock()
        mock_db.get_collection.return_value = mock_collection

        mock_result = Mock()
        mock_result.matched_count = 1
        mock_result.modified_count = 1
        mock_collection.update_one.return_value = mock_result

        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter.db = mock_db

        data = {'_id': 'doc_id'}
        result = adapter.delete(TEST_COLLECTION, data)

        mock_collection.update_one.assert_called_once_with(
            data,
            {'$set': {'active': False}}
        )
        self.assertTrue(result)

    @patch('rococo.data.mongodb.MongoClient')
    def test_delete_no_match(self, mock_mongo_client):
        """
        Test delete when no document matches.

        Verifies False is returned.
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        mock_db = Mock()
        mock_collection = Mock()
        mock_db.get_collection.return_value = mock_collection

        mock_result = Mock()
        mock_result.matched_count = 0
        mock_result.modified_count = 0
        mock_collection.update_one.return_value = mock_result

        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter.db = mock_db

        result = adapter.delete(TEST_COLLECTION, {'_id': 'nonexistent'})

        self.assertFalse(result)

    @patch('rococo.data.mongodb.MongoClient')
    def test_delete_error(self, mock_mongo_client):
        """
        Test delete with PyMongoError.

        Verifies RuntimeError is raised.
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        mock_db = Mock()
        mock_collection = Mock()
        mock_db.get_collection.return_value = mock_collection
        mock_collection.update_one.side_effect = errors.PyMongoError("Update failed")

        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter.db = mock_db

        with self.assertRaises(RuntimeError) as context:
            adapter.delete(TEST_COLLECTION, {'_id': 'doc_id'})

        self.assertIn("delete failed", str(context.exception))


class TestMongoDBAdapterInsertMany(unittest.TestCase):
    """Test insert_many method."""

    @patch('rococo.data.mongodb.MongoClient')
    def test_insert_many_success(self, mock_mongo_client):
        """
        Test successful bulk insert.

        Verifies:
        - _id fields are removed before insert
        - insert_many is called
        - Inserted documents are returned with generated IDs
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        mock_db = Mock()
        mock_collection = Mock()
        mock_db.get_collection.return_value = mock_collection

        docs_to_insert = [
            {'_id': 'old1', 'name': 'doc1'},
            {'_id': 'old2', 'name': 'doc2'}
        ]

        mock_insert_result = Mock()
        mock_insert_result.inserted_ids = ['new1', 'new2']
        mock_collection.insert_many.return_value = mock_insert_result

        # Mock find to return inserted docs
        inserted_docs = [
            {'_id': 'new1', 'name': 'doc1'},
            {'_id': 'new2', 'name': 'doc2'}
        ]
        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(return_value=iter(inserted_docs))
        mock_collection.find.return_value = mock_cursor

        mock_session = Mock()
        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter.db = mock_db
        adapter._session = mock_session

        result = adapter.insert_many(TEST_COLLECTION, docs_to_insert)

        # Verify _id was removed from insert call
        insert_call_args = mock_collection.insert_many.call_args
        inserted_docs_arg = insert_call_args[0][0]
        for doc in inserted_docs_arg:
            self.assertNotIn('_id', doc)

        # Verify result
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['_id'], 'new1')
        self.assertEqual(result[1]['_id'], 'new2')

    @patch('rococo.data.mongodb.MongoClient')
    def test_insert_many_empty_list(self, mock_mongo_client):
        """
        Test insert_many with empty list.

        Verifies ValueError is raised.
        """
        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)

        with self.assertRaises(ValueError) as context:
            adapter.insert_many(TEST_COLLECTION, [])

        self.assertIn("cannot be empty", str(context.exception))

    @patch('rococo.data.mongodb.MongoClient')
    def test_insert_many_error(self, mock_mongo_client):
        """
        Test insert_many with PyMongoError.

        Verifies RuntimeError is raised.
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        mock_db = Mock()
        mock_collection = Mock()
        mock_db.get_collection.return_value = mock_collection
        mock_collection.insert_many.side_effect = errors.PyMongoError("Insert failed")

        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter.db = mock_db
        adapter._session = Mock()

        with self.assertRaises(RuntimeError) as context:
            adapter.insert_many(TEST_COLLECTION, [{'name': 'test'}])

        self.assertIn("insert_many failed", str(context.exception))


class TestMongoDBAdapterCreateIndex(unittest.TestCase):
    """Test create_index method."""

    @patch('rococo.data.mongodb.MongoClient')
    def test_create_index_basic(self, mock_mongo_client):
        """
        Test basic index creation.

        Verifies create_index is called with columns and name.
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        mock_db = Mock()
        mock_collection = Mock()
        mock_db.get_collection.return_value = mock_collection
        mock_collection.create_index.return_value = 'test_index'

        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter.db = mock_db

        columns = [('name', 1), ('email', 1)]
        result = adapter.create_index(TEST_COLLECTION, columns, 'test_index')

        mock_collection.create_index.assert_called_once()
        call_args = mock_collection.create_index.call_args
        self.assertEqual(call_args[0][0], columns)
        self.assertEqual(call_args[1]['name'], 'test_index')
        self.assertEqual(result, 'test_index')

    @patch('rococo.data.mongodb.MongoClient')
    def test_create_index_with_partial_filter(self, mock_mongo_client):
        """
        Test index creation with partial filter.

        Verifies partial filter expression is included.
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        mock_db = Mock()
        mock_collection = Mock()
        mock_db.get_collection.return_value = mock_collection
        mock_collection.create_index.return_value = 'partial_index'

        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter.db = mock_db

        columns = [('active', 1)]
        partial_filter = {'active': True}
        adapter.create_index(TEST_COLLECTION, columns, 'partial_index', partial_filter=partial_filter)

        call_args = mock_collection.create_index.call_args
        self.assertEqual(call_args[1]['partialFilterExpression'], partial_filter)

    @patch('rococo.data.mongodb.MongoClient')
    def test_create_index_error(self, mock_mongo_client):
        """
        Test create_index with PyMongoError.

        Verifies RuntimeError is raised.
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        mock_db = Mock()
        mock_collection = Mock()
        mock_db.get_collection.return_value = mock_collection
        mock_collection.create_index.side_effect = errors.PyMongoError("Index creation failed")

        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter.db = mock_db

        with self.assertRaises(RuntimeError) as context:
            adapter.create_index(TEST_COLLECTION, [('name', 1)], 'test_index')

        self.assertIn("create_index failed", str(context.exception))


class TestMongoDBAdapterAggregate(unittest.TestCase):
    """Test aggregate method."""

    @patch('rococo.data.mongodb.MongoClient')
    def test_aggregate_success(self, mock_mongo_client):
        """
        Test successful aggregation pipeline execution.

        Verifies:
        - aggregate is called with pipeline
        - Results are returned as list
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        mock_db = Mock()
        mock_collection = Mock()
        mock_db.get_collection.return_value = mock_collection

        results = [{'count': 10}, {'count': 20}]
        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(return_value=iter(results))
        mock_collection.aggregate.return_value = mock_cursor

        mock_session = Mock()
        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter.db = mock_db
        adapter._session = mock_session

        pipeline = [{'$group': {'_id': '$category', 'count': {'$sum': 1}}}]
        result = adapter.aggregate(TEST_COLLECTION, pipeline)

        mock_collection.aggregate.assert_called_once_with(pipeline, session=mock_session)
        self.assertEqual(result, results)

    @patch('rococo.data.mongodb.MongoClient')
    def test_aggregate_error(self, mock_mongo_client):
        """
        Test aggregate with PyMongoError.

        Verifies RuntimeError is raised.
        """
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        mock_db = Mock()
        mock_collection = Mock()
        mock_db.get_collection.return_value = mock_collection
        mock_collection.aggregate.side_effect = errors.PyMongoError("Aggregation failed")

        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)
        adapter.db = mock_db
        adapter._session = Mock()

        with self.assertRaises(RuntimeError) as context:
            adapter.aggregate(TEST_COLLECTION, [])

        self.assertIn("aggregate failed", str(context.exception))


class TestMongoDBAdapterNotImplementedMethods(unittest.TestCase):
    """Test methods that raise NotImplementedError."""

    @patch('rococo.data.mongodb.MongoClient')
    def test_get_move_entity_to_audit_table_query(self, mock_mongo_client):
        """
        Test get_move_entity_to_audit_table_query raises NotImplementedError.

        This is a stub to satisfy abstract API.
        """
        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)

        with self.assertRaises(NotImplementedError) as context:
            adapter.get_move_entity_to_audit_table_query(TEST_COLLECTION, TEST_ENTITY_ID)

        self.assertIn("not supported", str(context.exception))

    @patch('rococo.data.mongodb.MongoClient')
    def test_get_save_query(self, mock_mongo_client):
        """
        Test get_save_query raises NotImplementedError.

        This is a stub to satisfy abstract API.
        """
        adapter = MongoDBAdapter(TEST_URI, TEST_DATABASE)

        with self.assertRaises(NotImplementedError) as context:
            adapter.get_save_query(TEST_COLLECTION, {'entity_id': TEST_ENTITY_ID})

        self.assertIn("not supported", str(context.exception))


if __name__ == '__main__':
    unittest.main()
