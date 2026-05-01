"""Tests for SQLMigrationBase and its dialect subclasses (Postgres, MySQL).

Verifies:
- Identifier and PK-keys validation
- Result-extraction helper
- Shared method skeletons call execute with expected SQL + parameterized args
- PostgresMigration and MySQLMigration produce dialect-correct SQL
"""

from unittest.mock import MagicMock

import pytest

from rococo.migrations.common.sql_migration_base import SQLMigrationBase
from rococo.migrations.postgres.migration import PostgresMigration
from rococo.migrations.mysql.migration import MySQLMigration


def _make_migration(cls):
    adapter = MagicMock()
    adapter._database = "testdb"
    instance = cls(adapter)
    instance.execute = MagicMock(return_value=None)
    return instance, adapter


# ---------- _validate_ident ----------

class TestValidateIdent:
    def setup_method(self):
        self.m, _ = _make_migration(PostgresMigration)

    @pytest.mark.parametrize("name", [
        "users", "user_email_idx", "_temp", "T", "x1", "table_2025",
    ])
    def test_accepts_standard_identifiers(self, name):
        assert self.m._validate_ident(name) == name

    @pytest.mark.parametrize("name", [
        "1table", "user-data", "user data", "user;DROP",
        "drop table users", "", "ta'ble", "ta\"ble",
    ])
    def test_rejects_bad_identifiers(self, name):
        with pytest.raises(ValueError, match="invalid SQL identifier"):
            self.m._validate_ident(name)

    @pytest.mark.parametrize("name", [None, 123, [], {}, b"users"])
    def test_rejects_non_string(self, name):
        with pytest.raises(ValueError, match="invalid SQL identifier"):
            self.m._validate_ident(name)


# ---------- _validate_pk_keys ----------

class TestValidatePkKeys:
    def setup_method(self):
        self.m, _ = _make_migration(PostgresMigration)

    @pytest.mark.parametrize("keys", [
        "(id)", "(id, email)", "(id, name, created_at)", "(_a, _b)",
    ])
    def test_accepts_valid_keys(self, keys):
        assert self.m._validate_pk_keys(keys) == keys

    @pytest.mark.parametrize("keys", [
        "id", "(id", "id)", "()1", "(id,)", "(1col)", "(col-name)",
        "(col; DROP)", "",
    ])
    def test_rejects_invalid_keys(self, keys):
        with pytest.raises(ValueError):
            self.m._validate_pk_keys(keys)


# ---------- _extract_count ----------

class TestExtractCount:
    def test_extracts_count_from_aliased_dict(self):
        assert SQLMigrationBase._extract_count([{"count": 5}]) == 5

    def test_extracts_count_from_mysql_style_key(self):
        # When SQL has no alias and MySQL returns COUNT(*) literal as key
        assert SQLMigrationBase._extract_count([{"COUNT(*)": 7}]) == 7

    def test_zero_when_empty(self):
        assert SQLMigrationBase._extract_count([]) == 0
        assert SQLMigrationBase._extract_count(None) == 0
        assert SQLMigrationBase._extract_count([{}]) == 0

    def test_handles_none_value(self):
        assert SQLMigrationBase._extract_count([{"count": None}]) == 0


# ---------- Shared method behavior (Postgres) ----------

class TestPostgresHelpers:
    def setup_method(self):
        self.m, self.adapter = _make_migration(PostgresMigration)

    def test_does_column_exist_uses_parameterized_query(self):
        self.m.execute = MagicMock(return_value=[{"count": 1}])
        result = self.m._does_column_exist("users", "email")

        assert result is True
        sql, kwargs = self.m.execute.call_args.args[0], self.m.execute.call_args.kwargs
        assert "information_schema.columns" in sql
        assert "%s" in sql
        assert kwargs["args"] == ("testdb", "users", "email")

    def test_does_column_exist_returns_false_when_zero(self):
        self.m.execute = MagicMock(return_value=[{"count": 0}])
        assert self.m._does_column_exist("users", "email") is False

    def test_does_pk_exists_uses_parameterized_query(self):
        self.m.execute = MagicMock(return_value=[{"count": 1}])
        self.m._does_primary_key_constraint_exists("users")
        sql, kwargs = self.m.execute.call_args.args[0], self.m.execute.call_args.kwargs
        assert "information_schema.table_constraints" in sql
        assert "PRIMARY KEY" in sql
        assert kwargs["args"] == ("testdb", "users")

    def test_add_column_skips_when_exists(self):
        self.m._does_column_exist = MagicMock(return_value=True)
        self.m.add_column("users", "email", "VARCHAR(255)")
        self.m.execute.assert_not_called()

    def test_add_column_emits_pg_dialect(self):
        self.m._does_column_exist = MagicMock(return_value=False)
        self.m.add_column("users", "email", "VARCHAR(255)")
        sql = self.m.execute.call_args.args[0]
        assert sql == "ALTER TABLE users ADD COLUMN email VARCHAR(255);"

    def test_drop_column_skips_when_missing(self):
        self.m._does_column_exist = MagicMock(return_value=False)
        self.m.drop_column("users", "email")
        self.m.execute.assert_not_called()

    def test_drop_column_emits_pg_dialect(self):
        self.m._does_column_exist = MagicMock(return_value=True)
        self.m.drop_column("users", "email")
        assert self.m.execute.call_args.args[0] == "ALTER TABLE users DROP COLUMN email;"

    def test_alter_index_runs_drop_then_create(self):
        self.m.alter_index("users", "new_idx", "email", "old_idx")
        calls = [c.args[0] for c in self.m.execute.call_args_list]
        assert calls == [
            "DROP INDEX IF EXISTS old_idx;",
            "CREATE INDEX new_idx ON users (email);",
        ]

    def test_add_index(self):
        self.m.add_index("users", "email_idx", "email")
        assert self.m.execute.call_args.args[0] == "CREATE INDEX email_idx ON users (email);"

    def test_remove_index(self):
        self.m.remove_index("users", "email_idx")
        assert self.m.execute.call_args.args[0] == "DROP INDEX IF EXISTS email_idx;"

    def test_alter_column(self):
        self.m.alter_column("users", "email", "TEXT")
        assert self.m.execute.call_args.args[0] == "ALTER TABLE users ALTER COLUMN email TYPE TEXT;"

    def test_change_column_name(self):
        self.m.change_column_name("users", "email", "user_email")
        assert self.m.execute.call_args.args[0] == "ALTER TABLE users RENAME COLUMN email TO user_email;"

    def test_change_table_name(self):
        self.m.change_table_name("users", "people")
        assert self.m.execute.call_args.args[0] == "ALTER TABLE users RENAME TO people;"

    def test_create_table_with_drop_uses_cascade(self):
        self.m.create_table("users", "id INT PRIMARY KEY")
        calls = [c.args[0] for c in self.m.execute.call_args_list]
        assert calls == [
            "DROP TABLE IF EXISTS users CASCADE;",
            "CREATE TABLE users (id INT PRIMARY KEY);",
        ]

    def test_create_table_without_drop(self):
        self.m.create_table("users", "id INT", drop_if_exists=False)
        calls = [c.args[0] for c in self.m.execute.call_args_list]
        assert calls == ["CREATE TABLE users (id INT);"]

    def test_drop_table_uses_cascade(self):
        self.m.drop_table("users")
        assert self.m.execute.call_args.args[0] == "DROP TABLE users CASCADE;"

    def test_remove_primary_key_emits_pg_dialect(self):
        self.m._does_primary_key_constraint_exists = MagicMock(return_value=True)
        self.m.remove_primary_key("users")
        assert self.m.execute.call_args.args[0] == \
            "ALTER TABLE users DROP CONSTRAINT IF EXISTS users_pkey;"

    def test_remove_primary_key_skips_when_missing(self):
        self.m._does_primary_key_constraint_exists = MagicMock(return_value=False)
        self.m.remove_primary_key("users")
        self.m.execute.assert_not_called()

    def test_add_primary_key_drops_then_adds(self):
        self.m._does_primary_key_constraint_exists = MagicMock(return_value=True)
        self.m.add_primary_key("users", "(id, email)")
        calls = [c.args[0] for c in self.m.execute.call_args_list]
        assert calls == [
            "ALTER TABLE users DROP CONSTRAINT IF EXISTS users_pkey;",
            "ALTER TABLE users ADD CONSTRAINT users_pkey PRIMARY KEY (id, email);",
        ]

    def test_add_primary_key_skips_drop_when_no_existing(self):
        self.m._does_primary_key_constraint_exists = MagicMock(return_value=False)
        self.m.add_primary_key("users", "(id)")
        calls = [c.args[0] for c in self.m.execute.call_args_list]
        assert calls == [
            "ALTER TABLE users ADD CONSTRAINT users_pkey PRIMARY KEY (id);"
        ]

    def test_update_version_table_parameterizes_value(self):
        self.m.update_version_table("0000000005")
        sql = self.m.execute.call_args.args[0]
        kwargs = self.m.execute.call_args.kwargs
        assert sql == "UPDATE db_version SET version = %s;"
        assert kwargs["args"] == ("0000000005",)

    def test_insert_db_version_data(self):
        self.m.insert_db_version_data()
        assert self.m.execute.call_args.args[0] == \
            "INSERT INTO db_version (version) VALUES ('0000000000');"


# ---------- Shared method behavior (MySQL) ----------

class TestMySQLDialect:
    def setup_method(self):
        self.m, self.adapter = _make_migration(MySQLMigration)

    def test_does_column_exist_uses_mysql_schema_query(self):
        self.m.execute = MagicMock(return_value=[{"count": 1}])
        self.m._does_column_exist("users", "email")
        sql, kwargs = self.m.execute.call_args.args[0], self.m.execute.call_args.kwargs
        assert "information_schema.COLUMNS" in sql
        assert "TABLE_SCHEMA" in sql
        assert "%s" in sql
        assert kwargs["args"] == ("testdb", "users", "email")

    def test_add_column_uses_mysql_dialect(self):
        self.m._does_column_exist = MagicMock(return_value=False)
        self.m.add_column("users", "email", "VARCHAR(255)")
        # MySQL: ADD (no COLUMN keyword)
        assert self.m.execute.call_args.args[0] == "ALTER TABLE users ADD email VARCHAR(255);"

    def test_drop_column_uses_mysql_dialect(self):
        self.m._does_column_exist = MagicMock(return_value=True)
        self.m.drop_column("users", "email")
        # MySQL: DROP (no COLUMN keyword)
        assert self.m.execute.call_args.args[0] == "ALTER TABLE users DROP email;"

    def test_alter_index_is_atomic(self):
        self.m.alter_index("users", "new_idx", "email", "old_idx")
        # MySQL: single ALTER TABLE with both ADD INDEX and DROP INDEX
        calls = [c.args[0] for c in self.m.execute.call_args_list]
        assert calls == [
            "ALTER TABLE users ADD INDEX new_idx (email), DROP INDEX old_idx;"
        ]

    def test_remove_index_uses_mysql_dialect(self):
        self.m.remove_index("users", "email_idx")
        assert self.m.execute.call_args.args[0] == "ALTER TABLE users DROP INDEX email_idx;"

    def test_add_index_uses_mysql_dialect(self):
        self.m.add_index("users", "email_idx", "email")
        assert self.m.execute.call_args.args[0] == \
            "ALTER TABLE users ADD INDEX email_idx (email);"

    def test_alter_column_uses_modify(self):
        self.m.alter_column("users", "email", "TEXT")
        assert self.m.execute.call_args.args[0] == \
            "ALTER TABLE users MODIFY COLUMN email TEXT;"

    def test_drop_table_no_cascade(self):
        self.m.drop_table("users")
        assert self.m.execute.call_args.args[0] == "DROP TABLE users;"

    def test_create_table_with_drop_no_cascade(self):
        self.m.create_table("users", "id INT")
        calls = [c.args[0] for c in self.m.execute.call_args_list]
        assert calls == [
            "DROP TABLE IF EXISTS users;",
            "CREATE TABLE users (id INT);",
        ]

    def test_remove_primary_key_uses_mysql_dialect(self):
        self.m._does_primary_key_constraint_exists = MagicMock(return_value=True)
        self.m.remove_primary_key("users")
        assert self.m.execute.call_args.args[0] == "ALTER TABLE users DROP PRIMARY KEY;"

    def test_add_primary_key_uses_mysql_dialect(self):
        self.m._does_primary_key_constraint_exists = MagicMock(return_value=False)
        self.m.add_primary_key("users", "(id)")
        assert self.m.execute.call_args.args[0] == "ALTER TABLE users ADD PRIMARY KEY (id);"


# ---------- Identifier injection is blocked ----------

class TestInjectionBlocked:
    """All identifier-accepting methods must reject injection-shaped names."""

    def setup_method(self):
        self.m, _ = _make_migration(PostgresMigration)

    def test_add_column_rejects_injection_in_table(self):
        with pytest.raises(ValueError):
            self.m.add_column("users; DROP TABLE users--", "email", "TEXT")

    def test_add_column_rejects_injection_in_column(self):
        with pytest.raises(ValueError):
            self.m.add_column("users", "email'); DROP TABLE users--", "TEXT")

    def test_drop_table_rejects_injection(self):
        with pytest.raises(ValueError):
            self.m.drop_table("users; DROP TABLE secrets")

    def test_create_table_rejects_injection(self):
        with pytest.raises(ValueError):
            self.m.create_table("users; SELECT 1", "id INT")

    def test_change_table_name_rejects_injection(self):
        with pytest.raises(ValueError):
            self.m.change_table_name("users", "people; DROP TABLE roles")

    def test_add_primary_key_rejects_injection_in_keys(self):
        with pytest.raises(ValueError):
            self.m.add_primary_key("users", "(id); DROP TABLE users--")
