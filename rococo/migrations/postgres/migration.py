import logging
from ..common.migration_base import MigrationBase
from rococo.data.postgresql import PostgreSQLAdapter

class PostgresMigration(MigrationBase):
    def __init__(self, db_adapter: PostgreSQLAdapter):
        super().__init__(db_adapter)

    def _does_column_exist(self, table_name, column_name, commit: bool = True):
        table_catalog = self.db_adapter._database

        query = f"""
        SELECT COUNT(*) 
        FROM information_schema.columns 
        WHERE table_catalog = '{table_catalog}'
          AND table_name = '{table_name}'
          AND column_name = '{column_name}';
        """
        response = self.execute(query, commit=commit)
        return int(response[0].get("count", 0)) > 0

    def _does_primary_key_constraint_exists(self, table_name, commit: bool = True):
        table_catalog = self.db_adapter._database

        query = f"""
        SELECT COUNT(*) 
        FROM information_schema.table_constraints 
        WHERE table_catalog = '{table_catalog}'
          AND table_name = '{table_name}'
          AND constraint_type = 'PRIMARY KEY';
        """
        response = self.execute(query, commit=commit)
        return int(response[0].get("count", 0)) > 0

    def remove_primary_key(self, table_name, commit: bool = True):
        if self._does_primary_key_constraint_exists(table_name, commit=commit):
            # Assuming the primary key constraint follows the convention: <table_name>_pkey
            query = f"ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS {table_name}_pkey;"
            return self.execute(query, commit=commit)
        logging.info(
            f"PRIMARY KEY constraint for table {table_name} does not exist. Skipping removal..."
        )

    def add_primary_key(self, table_name, keys: str, commit: bool = True):
        if self._does_primary_key_constraint_exists(table_name, commit=commit):
            query = f"ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS {table_name}_pkey;"
            self.execute(query, commit=commit)
        # 'keys' should be provided as a string like "(column1, column2)"
        query = f"ALTER TABLE {table_name} ADD CONSTRAINT {table_name}_pkey PRIMARY KEY {keys};"
        return self.execute(query, commit=commit)

    def add_column(self, table_name, column_name, datatype, commit: bool = True):
        if self._does_column_exist(table_name, column_name, commit=commit):
            logging.info(f"Column {column_name} for table {table_name} already exists. Skipping creation...")
            return
        query = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {datatype};"
        self.execute(query, commit=commit)

    def drop_column(self, table_name, column_name, commit: bool = True):
        if not self._does_column_exist(table_name, column_name, commit=commit):
            logging.info(f"Column {column_name} for table {table_name} does not exist. Skipping deletion...")
            return
        query = f"ALTER TABLE {table_name} DROP COLUMN {column_name};"
        self.execute(query, commit=commit)

    def alter_index(self, table_name, new_index_name, new_indexed_column, old_index_name, commit: bool = True):
        # PostgreSQL requires separate operations for dropping and creating indexes.
        drop_query = f"DROP INDEX IF EXISTS {old_index_name};"
        self.execute(drop_query, commit=commit)
        create_query = f"CREATE INDEX {new_index_name} ON {table_name} ({new_indexed_column});"
        self.execute(create_query, commit=commit)

    def add_index(self, table_name, index_name, indexed_column, commit: bool = True):
        query = f"CREATE INDEX {index_name} ON {table_name} ({indexed_column});"
        self.execute(query, commit=commit)

    def remove_index(self, table_name, index_name, commit: bool = True):
        query = f"DROP INDEX IF EXISTS {index_name};"
        self.execute(query, commit=commit)

    def alter_column(self, table_name, column_name, datatype, commit: bool = True):
        query = f"ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE {datatype};"
        self.execute(query, commit=commit)

    def change_column_name(self, table_name, old_column_name, new_column_name, commit: bool = True):
        query = f"ALTER TABLE {table_name} RENAME COLUMN {old_column_name} TO {new_column_name};"
        self.execute(query, commit=commit)

    def change_table_name(self, old_table, new_table, commit: bool = True):
        query = f"ALTER TABLE {old_table} RENAME TO {new_table};"
        self.execute(query, commit=commit)

    def create_table(self, table_name, fields_with_type, drop_if_exists=True, commit: bool = True):
        if drop_if_exists:
            # Using CASCADE to drop dependent objects, adjust if needed
            drop_query = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
            self.execute(drop_query, commit=commit)
        create_query = f"CREATE TABLE {table_name} ({fields_with_type});"
        self.execute(create_query, commit=commit)

    def drop_table(self, table_name, commit: bool = True):
        query = f"DROP TABLE {table_name} CASCADE;"
        self.execute(query, commit=commit)

    def insert_db_version_data(self, commit: bool = True):
        query = "INSERT INTO db_version (version) VALUES ('0000000000');"
        self.execute(query, commit=commit)

    def update_version_table(self, version, commit: bool = True):
        query = f"UPDATE db_version SET version = '{version}';"
        self.execute(query, commit=commit)
