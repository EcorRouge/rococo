import pytest
from unittest.mock import MagicMock
from rococo.migrations.mysql.migration import MySQLMigration

class TestMySQLMigration:
    @pytest.fixture
    def mock_db_adapter(self):
        adapter = MagicMock()
        adapter.execute_query.return_value = []
        adapter._connection = MagicMock()
        return adapter

    @pytest.fixture
    def migration(self, mock_db_adapter):
        return MySQLMigration(mock_db_adapter)

    def test_insert_db_version_data(self, migration):
        migration.insert_db_version_data()
        
        # Verify execute was called with correct non-f-string query
        expected_query = "INSERT INTO db_version (version) VALUES (0000000000);"
        migration.db_adapter.execute_query.assert_called()
        call_args = migration.db_adapter.execute_query.call_args
        assert call_args[0][0] == expected_query

    def test_update_version_table(self, migration):
        version = "2023010100"
        migration.update_version_table(version)
        
        # Verify execute was called with parameterized query
        expected_query = "UPDATE db_version SET version = %s;"
        migration.db_adapter.execute_query.assert_called()
        call_args = migration.db_adapter.execute_query.call_args
        assert call_args[0][0] == expected_query
        assert call_args[0][1] == (version,)

    def test_cursor_to_dict(self):
        # Setup
        description = (
            ('id', 3, None, None, None, None, 0),
            ('name', 253, None, None, None, None, 1)
        )
        result = (1, 'Test Name')
        
        # Execute
        data = MySQLMigration._cursor_to_dict(result, description)
        
        # Verify
        assert data == {'id': 1, 'name': 'Test Name'}

    def test_cursor_to_dict_empty(self):
        description = ()
        result = ()
        data = MySQLMigration._cursor_to_dict(result, description)
        assert data == {}
