import os
from importlib import import_module

from .migration_template import get_template
from .migration import Migration


SCRIPTS_DIR = os.path.abspath(os.path.dirname(__file__))
MIGRATION_DIR = os.path.abspath(os.path.dirname(SCRIPTS_DIR))



class MigrationRunner:

    def __init__(self, migrations_dir, db_adapter):
        self.migrations_dir = migrations_dir
        self.db_adapter = db_adapter
        self.migration = Migration(db_adapter)

    def _get_migration_scripts(self):
        script_versions = {
            file for file in os.listdir(self.migrations_dir) if file not in ["__init__.py", "lib", "__pycache__"]
        }
        return script_versions

    def get_db_version(self):
        query = f"""SELECT version from db_version;"""
        db_version = None
        try:
            with self.db_adapter:
                db_response = self.db_adapter.execute_query(query)
                results = self.db_adapter.parse_db_response(db_response)
                db_version = results[0].get('version')
        except Exception as e:
            raise e

        db_version = db_version or '0000000000'
        return f'{int(db_version):010d}'

    def _get_forward_migration_script(self):
        db_version = self.get_db_version()
        for file in self._get_migration_scripts():
            if file.strip().split('_')[1] == db_version:
                return file.strip().split('.')[0]


    def _get_backward_migration_script(self):
        db_version = self.get_db_version()
        for file in self._get_migration_scripts():
            if file.strip().split('_')[0] == db_version:
                return file.strip().split('.')[0]

    def create_migration_file(self):
        try:
            current_db_version = int(self.get_db_version())
        except:
            print('DB version not found in table!!!')
            return
        new_version = current_db_version + 1
        current_db_version = f'{current_db_version:010d}'
        new_version = f'{new_version:010d}'
        file_name = f"{new_version}_{current_db_version}_migration.py"
        template = get_template(new_version, current_db_version)
        with open(os.path.join(self.migrations_dir, file_name), 'w') as fp:
            fp.write(template)
        print(f"Created new migartion file at {os.path.join(self.migrations_dir, file_name)}")


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
