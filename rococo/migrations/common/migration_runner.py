import os
from importlib import import_module
from .migration_template import get_template

class MigrationRunner:
    def __init__(self, migrations_dir, migration):
        self.migrations_dir = migrations_dir
        self.migration = migration

    def _get_migration_scripts(self):
        # Return all files in the migration directory, ignoring some known ones
        return {
            file for file in os.listdir(self.migrations_dir)
            if file not in ["__init__.py", "lib", "__pycache__"]
        }

    def get_db_version(self):
        query = "SELECT version from db_version;"
        db_version = None
        try:
            with self.migration.db_adapter:
                db_response = self.migration.db_adapter.execute_query(query)
                results = self.migration.db_adapter.parse_db_response(db_response)
                db_version = results[0].get('version')
        except Exception as e:
            raise e

        db_version = db_version or '0000000000'
        return f'{int(db_version):010d}'

    def _get_forward_migration_script(self):
        db_version = self.get_db_version()
        for file in self._get_migration_scripts():
            # Assuming migration file naming convention: newversion_currentversion_migration.py
            parts = file.strip().split('_')
            if len(parts) > 1 and parts[1] == db_version:
                return file.strip().split('.')[0]

    def _get_backward_migration_script(self):
        db_version = self.get_db_version()
        for file in self._get_migration_scripts():
            parts = file.strip().split('_')
            if parts and parts[0] == db_version:
                return file.strip().split('.')[0]

    def create_migration_file(self):
        try:
            current_db_version = int(self.get_db_version())
        except Exception:
            print('DB version not found in table!!!')
            return
        new_version = current_db_version + 1
        current_db_version_str = f'{current_db_version:010d}'
        new_version_str = f'{new_version:010d}'
        file_name = f"{new_version_str}_{current_db_version_str}_migration.py"
        template = get_template(new_version_str, current_db_version_str)
        with open(os.path.join(self.migrations_dir, file_name), 'w') as fp:
            fp.write(template)
        print(f"Created new migration file at {os.path.join(self.migrations_dir, file_name)}")

    def run_forward_migration_script(self, old_db_version):
        file = self._get_forward_migration_script()
        if file is None:
            latest_db_version = self.get_db_version()
            if old_db_version == latest_db_version:
                print('Migration for current DB version not found!!!')
            else:
                print('Migration complete!!!')
                print(f'Latest DB version: {latest_db_version}')
            return
        print(f'Running forward migration: {file.strip().split("/")[-1]}')
        imported = import_module(f'{file}', package=None)
        imported.upgrade(self.migration)
        # Recursively run forward migrations until complete
        return self.run_forward_migration_script(old_db_version)

    def run_backward_migration_script(self):
        file = self._get_backward_migration_script()
        if file is None:
            print('Migration for current DB version not found!!!')
            return
        print(f'Running backward migration: {file.strip().split("/")[-1]}')
        imported = import_module(f'{file}', package=None)
        imported.downgrade(self.migration)
        print('Migration complete!!!')
        print(f'Latest DB version: {self.get_db_version()}')
