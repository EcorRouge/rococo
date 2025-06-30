import os
import logging
from importlib import import_module
# Assuming migration_template.py is in the same directory or accessible in python path
# For a common module, it would typically be:
# from rococo.migrations.common.migration_template import get_template
from .migration_template import get_template # This relative import is fine if it's part of a package

class MigrationRunner:
    def __init__(self, migrations_dir, migration):
        self.migrations_dir = migrations_dir
        self.migration = migration # migration is an instance of MongoMigration or SQL equivalent

    def _get_migration_scripts(self):
        # Return all files in the migration directory, ignoring some known ones
        return {
            file for file in os.listdir(self.migrations_dir)
            if file.endswith(".py") and file not in ["__init__.py", "lib", "__pycache__"]
        }

    def get_db_version(self):
        db_version = '0000000000' # Initialize with a default
        try:
            with self.migration.db_adapter: # Ensures adapter context (e.g., connection/session)
                # If MongoMigration is used, its get_current_db_version will be called.
                # For SQL, a similar method would exist or be adapted.
                if hasattr(self.migration, 'get_current_db_version'):
                    # This path is for migration classes that define their own version retrieval
                    db_version = self.migration.get_current_db_version()
                else:
                    # This is the fallback for SQL-based migrations using a 'db_version' table
                    query = "SELECT version from db_version;"
                    db_response = self.migration.db_adapter.execute_query(query)
                    results = self.migration.db_adapter.parse_db_response(db_response)
                    if results and isinstance(results, list) and len(results) > 0:
                        db_version = results[0].get('version')
                    elif results and isinstance(results, dict): # SurrealDB parse_db_response can return a dict
                        db_version = results.get('version')
                    # If results is empty or version key is missing, db_version remains default or None

        except Exception as e:
            logging.warning(f"Could not retrieve DB version, assuming '0000000000'. Error: {e}", exc_info=True)
            db_version = '0000000000'

        db_version = db_version or '0000000000' # Ensure it's not None

        try:
            # Attempt to format as 10-digit string if it's an integer or integer-like string
            return f'{int(db_version):010d}'
        except ValueError:
            # If db_version is not purely numeric (e.g. might be a hash or already formatted string from Mongo)
            # Log an info message rather than an error if this is expected for some DB types.
            # Or, the migration systems should agree on the format.
            # If MongoMigration.get_current_db_version() already returns a formatted string, int() will fail.
            # Let's assume MongoMigration.get_current_db_version() will return a string that can be int()'d or is already formatted.
            # For robustness, check if it's already in the desired format.            
            if isinstance(db_version, str) and len(db_version) == 10 and db_version.isdigit():
                return db_version
            logging.warning(f"DB version '{db_version}' is not a simple integer string. Standardize format or using as is.")
            return db_version

    def _get_forward_migration_script(self):
        db_version_str = self.get_db_version()
        migration_scripts = self._get_migration_scripts()

        # Sort scripts to ensure deterministic order if multiple match (though unlikely with current naming)
        # File naming convention: newversion_currentversion_description.py
        # We want the script where 'currentversion' matches db_version_str
        # And we want the one with the numerically smallest 'newversion' that is > db_version_str
                
        candidate_scripts = []
        for file in migration_scripts:
            filename_no_ext = os.path.splitext(file)[0] # More robust way to get filename without ext
            parts = filename_no_ext.split('_')
            # Expecting at least 3 parts: new_rev, down_rev, name
            if len(parts) >= 2 and parts[1] == db_version_str:
                # parts[0] is the new_version for this script
                candidate_scripts.append({'new_version': parts[0], 'filename': filename_no_ext})

        if not candidate_scripts:
            return None

        # Sort candidates by their 'new_version' numerically to pick the next logical step
        candidate_scripts.sort(key=lambda x: int(x['new_version']))
        
        # The first script after sorting by new_version should be the correct one.
        # Additional check: new_version should be greater than current db_version_str numerically.
        # Though the naming convention itself (parts[1] == db_version_str) implies this.        
        return candidate_scripts[0]['filename']

    def _get_backward_migration_script(self):
        db_version_str = self.get_db_version()
        migration_scripts = self._get_migration_scripts()

        # File naming convention: currentversion_targetversion_description.py
        # We want the script where 'currentversion' (parts[0]) matches db_version_str
        candidate_scripts = []
        for file in migration_scripts:
            filename_no_ext = os.path.splitext(file)[0]
            parts = filename_no_ext.split('_')
            if len(parts) >= 2 and parts[0] == db_version_str:
                candidate_scripts.append({'target_version': parts[1], 'filename': filename_no_ext})
        
        if not candidate_scripts:
            return None

        # If there are multiple, it implies an ambiguous state or bad naming.
        # For simplicity, assume one valid backward script. If more sophisticated logic needed,
        # it could sort by target_version to pick the one that goes "most recently" back.
        # Typically, there's only one direct downgrade path.
        if len(candidate_scripts) > 1:
            logging.warning(f"Multiple backward migration scripts found for version {db_version_str}. Using the first one found: {candidate_scripts[0]['filename']}")
        
        return candidate_scripts[0]['filename']

    def create_migration_file(self):
        current_db_version_str = self.get_db_version()
        try:
            current_db_version_int = int(current_db_version_str)
        except ValueError:
            # This can happen if get_db_version returns a non-integer string
            # (e.g. if Mongo's version was a hash, though we try to format it)            
            logging.error(f"Cannot create new migration. Current DB version '{current_db_version_str}' is not a parseable integer.")
            # This is a user-facing error, so print is appropriate if CLI directly calls this
            print(f"Error: DB version '{current_db_version_str}' is not a valid integer. Cannot determine next version.")
            return

        new_version_int = current_db_version_int + 1
        # Formatting to 0-padded string
        new_version_str_formatted = f'{new_version_int:010d}'
        current_db_version_str_formatted = f'{current_db_version_int:010d}'

        # Allow user to provide a descriptive name for the migration
        description = input("Enter a short snake_case description for the migration (e.g., add_user_email_index): ").strip().replace(" ", "_")
        if not description:
            description = "migration"

        file_name = f"{new_version_str_formatted}_{current_db_version_str_formatted}_{description}.py"
        template = get_template(new_version_str_formatted, current_db_version_str_formatted)
        file_path = os.path.join(self.migrations_dir, file_name)
        
        try:
            with open(file_path, 'w') as fp:
                fp.write(template)
            # This message is direct feedback for a CLI command.
            print(f"Created new migration file at {file_path}")
        except IOError as e:
            logging.error(f"Error creating migration file '{file_path}': {e}", exc_info=True)
            # Also print for CLI user
            print(f"Error creating migration file: {e}")

    def run_forward_migration_script(self, initial_db_version_for_run):
        # Renamed old_db_version to initial_db_version_for_run for clarity
        current_script_filename = self._get_forward_migration_script()

        if current_script_filename is None:
            latest_db_version_after_ops = self.get_db_version()
            if initial_db_version_for_run == latest_db_version_after_ops:
                # This means no scripts were found that could run from initial_db_version_for_run
                logging.info(f'No forward migration scripts found for current DB version: {initial_db_version_for_run}. Database is likely up to date.')
            else:
                # This means some migrations ran, and now no more are found.
                logging.info('All pending forward migrations complete!')
            logging.info(f'Latest DB version: {latest_db_version_after_ops}')
            return # End of migration run

        # Using os.path.basename to be platform-agnostic, though split('/') works on POSIX
        logging.info(f'Running forward migration: {os.path.basename(current_script_filename)}.py')
        
        try:
            # Dynamically import the migration module
            # Assumes migrations_dir is in sys.path (BaseCli handles this)            
            module_name = current_script_filename
            migration_module = import_module(module_name) # Assumes migrations_dir is in sys.path
            migration_module.upgrade(self.migration)
        except Exception as e:
            logging.error(f"Error during forward migration {current_script_filename}.py: {e}", exc_info=True)
            # This print is for direct CLI feedback during a run
            print(f"Error applying migration {current_script_filename}.py. Halting.")
            # Potentially offer to show current DB version or suggest manual check
            print(f"Current DB version after error: {self.get_db_version()}")
            return # Stop further migrations

        # Recursively run next forward migration
        # The `initial_db_version_for_run` remains the version from the start of this `rf` command.
        # The next call to `_get_forward_migration_script` will use the *new* current DB version
        # (updated by the migration script that just ran).
        return self.run_forward_migration_script(initial_db_version_for_run)

    def run_backward_migration_script(self):
        current_script_filename = self._get_backward_migration_script()

        if current_script_filename is None:
            logging.info(f'No backward migration script found for current DB version: {self.get_db_version()}.')
            return

        logging.info(f'Running backward migration: {os.path.basename(current_script_filename)}.py')
        
        try:
            module_name = current_script_filename
            migration_module = import_module(module_name)
            migration_module.downgrade(self.migration)
        except Exception as e:
            logging.error(f"Error during backward migration {current_script_filename}.py: {e}", exc_info=True)
            # Direct CLI feedback
            print(f"Error applying downgrade migration {current_script_filename}.py. Halting.")
            print(f"Current DB version after error: {self.get_db_version()}")
            return

        # Unlike forward, backward usually runs one step at a time.
        # If you wanted chain downgrades, this would also be recursive.
        # For now, one step seems standard for 'rb'.
        logging.info('Backward migration script complete.')
        logging.info(f'DB version is now: {self.get_db_version()}')