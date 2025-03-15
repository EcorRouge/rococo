import logging
from ..common.migration_base import MigrationBase
from rococo.data.mysql import MySqlAdapter


class MySQLMigration(MigrationBase):
    def __init__(self, db_adapter: MySqlAdapter):
        super().__init__(db_adapter)

    def _does_column_exist(self, table_name, column_name, commit: bool = True):
        schema_name = self.db_adapter._database
        query = f"""
        SELECT COUNT(*) 
            FROM information_schema.COLUMNS 
            WHERE 
                TABLE_SCHEMA = '{schema_name}' 
            AND TABLE_NAME = '{table_name}' 
            AND COLUMN_NAME = '{column_name}';
        """
        response = self.execute(query, commit=commit)
        return response[0].get("COUNT(*)") > 0

    def _does_primary_key_constraint_exists(self, table_name, commit: bool = True):
        schema_name = self.db_adapter._database
        query = f"""
        SELECT COUNT(*) 
            FROM information_schema.TABLE_CONSTRAINTS 
            WHERE 
                TABLE_SCHEMA = '{schema_name}' 
            AND TABLE_NAME = '{table_name}' 
            AND CONSTRAINT_TYPE = 'PRIMARY KEY';
        """
        response = self.execute(query, commit=commit)
        return response[0].get("COUNT(*)") > 0

    def remove_primary_key(self, table_name, commit: bool = True):
        if self._does_primary_key_constraint_exists(table_name, commit=commit):
            query = f"""ALTER TABLE {table_name} DROP PRIMARY KEY;"""
            return self.execute(query, commit)

        logging.info(
            f"PRIMARY KEY constraints for table {table_name} does not exist. Skipping remove primary key..."
        )

    def add_primary_key(self, table_name, keys: str, commit: bool = True):
        if self._does_primary_key_constraint_exists(table_name, commit=commit):
            query = f"""ALTER TABLE {table_name} DROP PRIMARY KEY;"""
            self.execute(query, commit)

        query = f"""ALTER TABLE {table_name} ADD PRIMARY KEY {keys};"""
        return self.execute(query, commit)

    def add_column(self, table_name, column_name, datatype, commit: bool = True):
        if self._does_column_exist(table_name, column_name, commit=commit):
            logging.info(f"Column {column_name} for table {table_name} already exists. Skipping creation...")
            return
        query = f"""ALTER TABLE {table_name} ADD {column_name} {datatype};"""
        self.execute(query, commit)

    def drop_column(self, table_name, column_name, commit: bool = True):
        if not self._does_column_exist(table_name, column_name, commit=commit):
            logging.info(f"Column {column_name} for table {table_name} does not exist. Skipping deletion...")
            return
        query = f"""ALTER TABLE {table_name} DROP {column_name};"""
        self.execute(query, commit=commit)

    def alter_index(self, table_name, new_index_name, new_indexed_column, old_index_name, commit: bool = True):
        query = f"""ALTER TABLE {table_name} ADD INDEX {new_index_name} ({new_indexed_column}), DROP INDEX {old_index_name};"""
        self.execute(query, commit=commit)

    def add_index(self, table_name, index_name, indexed_column, commit: bool = True):
        query = f"""ALTER TABLE {table_name} ADD INDEX {index_name} ({indexed_column});"""
        self.execute(query, commit=commit)

    def remove_index(self, table_name, index_name, commit: bool = True):
        query = f"""ALTER TABLE {table_name} DROP INDEX {index_name};"""
        self.execute(query, commit=commit)

    def alter_column(self, table_name, column_name, datatype, commit: bool = True):
        query = f"""ALTER TABLE {table_name} MODIFY COLUMN {column_name} {datatype};"""
        self.execute(query, commit=commit)

    def change_column_name(self, table_name, old_column_name, new_column_name, commit: bool = True):
        query = f"""ALTER TABLE {table_name} RENAME COLUMN {old_column_name} TO {new_column_name};"""
        self.execute(query, commit=commit)

    def change_table_name(self, old_table, new_table, commit: bool = True):
        query = f"""ALTER TABLE {old_table} RENAME TO {new_table};"""
        self.execute(query, commit=commit)

    def create_table(self, table_name, fields_with_type, drop_if_exists=True, commit: bool = True):
        if drop_if_exists:
            drop_query = f"""DROP TABLE IF EXISTS {table_name};"""
            self.execute(drop_query, commit=commit)
        create_query = f"""CREATE TABLE {table_name} ({fields_with_type});"""
        self.execute(create_query, commit=commit)

    def drop_table(self, table_name, commit: bool = True):
        query = f"""DROP TABLE {table_name};"""
        self.execute(query, commit=commit)

    def insert_db_version_data(self, commit: bool = True):
        query = f"INSERT INTO db_version (version) VALUES (0000000000);"
        self.execute(query, commit=commit)

    def update_version_table(self, version, commit: bool = True):
        query = f"UPDATE db_version SET version = {version};"
        self.execute(query, commit=commit)

    @staticmethod
    def _cursor_to_dict(result, description):
        return dict(zip([col[0] for col in description], result))
