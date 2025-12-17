import argparse
import os
import sys
from collections import OrderedDict
from dotenv import dotenv_values
from abc import abstractmethod, ABC

class BaseCli(ABC):
    # These class attributes must be provided by the subclass
    DB_TYPE = None                # 'mysql' or 'postgres'
    REQUIRED_ENV_VARS = []        # List of env variable names
    ADAPTER_CLASS = None          # e.g. MySqlAdapter or PostgresAdapter
    MIGRATION_CLASS = None        # e.g. MySQLMigration or PostgresMigration
    ENV_SECRETS_FILE = '.env.secrets'

    def __init__(self):
        self.parser = self._create_parser()

    def _create_parser(self):
        parser = argparse.ArgumentParser(
            description=f"Rococo CLI interface for {self.DB_TYPE} migrations."
        )
        parser.add_argument(
            '--migrations-dir',
            type=str,
            help="Path to migrations directory.",
            default=None
        )
        parser.add_argument(
            '--env-files',
            type=str,
            nargs='+',
            help="Path to environment files.",
            default=[]
        )
        # Define subcommands
        subparsers = parser.add_subparsers(dest="command", help="Subcommand to run")
        subparsers.add_parser('new', help="Create new migration file.")
        subparsers.add_parser('rf', help="Run forward migration.")
        subparsers.add_parser('rb', help="Run backward migration.")
        subparsers.add_parser('version', help="Get DB version.")
        return parser

    def _load_from_secrets(self):
        """Helper to load env vars from secrets file if needed."""
        if os.path.exists(self.ENV_SECRETS_FILE) and os.path.isfile(self.ENV_SECRETS_FILE):
            secrets_env = dotenv_values(self.ENV_SECRETS_FILE)
            app_env_name = secrets_env.get('APP_ENV')

            if app_env_name and os.path.exists(f'{app_env_name}.env') and os.path.isfile(f'{app_env_name}.env'):
                app_env = dotenv_values(f'{app_env_name}.env')
                return OrderedDict(list(secrets_env.items()) + list(app_env.items()))
            else:
                self.parser.error(f'APP_ENV not found in {self.ENV_SECRETS_FILE} or corresponding .env file missing.')
                return None
        else:
            self.parser.error(f'{self.ENV_SECRETS_FILE} not found. Specify env file(s) with --env-files.')
            return None

    def _load_from_cli_args(self, args):
        """Helper to load env vars from files specified in CLI args."""
        merged_env = []
        for env_file in args.env_files:
            if os.path.exists(env_file) and os.path.isfile(env_file):
                merged_env += list(dotenv_values(env_file).items())
            else:
                self.parser.error(f"{env_file} file not found.")
                return None
        return OrderedDict(merged_env)

    def load_env(self, args):
        """
        Load environment variables.
        If --env-files is provided, load from them.
        Otherwise, check if required vars are present in os.environ.
        If missing, try to load from .env.secrets and corresponding APP_ENV file.
        """
        # Load env files if specified
        if args.env_files:
            return self._load_from_cli_args(args)

        # First, check environment variables in the current environment
        missing_vars = [var for var in self.REQUIRED_ENV_VARS if not os.getenv(var)]
        
        if not missing_vars:
             # If all required environment variables are present
            return {var: os.getenv(var) for var in self.REQUIRED_ENV_VARS}
            
        # If any required env vars are missing, check .env files
        print(f"Missing environment variables: {', '.join(missing_vars)}. Trying to load from .env files...")
        return self._load_from_secrets()

    def get_migrations_dir(self, args):
        migrations_dir = None
        if args.migrations_dir and os.path.exists(args.migrations_dir) and os.path.isdir(args.migrations_dir):
            migrations_dir = args.migrations_dir
        else:
            # Try some default locations
            for _dir in ['flask/app/migrations', 'api/app/migrations', 'app/migrations']:
                if os.path.exists(_dir) and os.path.isdir(_dir):
                    migrations_dir = _dir
                    break
        if not migrations_dir:
            self.parser.error("No migrations directory found.")
        return migrations_dir

    @abstractmethod
    def get_db_adapter(self, merged_env):
        pass

    def get_migration(self, args):
        merged_env = self.load_env(args)
        if merged_env is None:
            self.parser.error("Unable to load env.")
            return
        db_adapter = self.get_db_adapter(merged_env)
        return self.MIGRATION_CLASS(db_adapter)

    def run(self):
        args = self.parser.parse_args()
        migrations_dir = self.get_migrations_dir(args)
        
        sys.path.append(migrations_dir)
        migration = self.get_migration(args)
        # Import the common runner and instantiate it with the provided migration class.
        from .migration_runner import MigrationRunner
        runner = MigrationRunner(migrations_dir, migration)
        
        db_version = runner.get_db_version()
        print(f"Current DB version: {db_version}")
        if args.command == 'new':
            runner.create_migration_file()
        elif args.command == 'rf':
            runner.run_forward_migration_script(db_version)
        elif args.command == 'rb':
            runner.run_backward_migration_script()
        elif args.command == 'version':
            print(f"DB version: {db_version}")
        else:
            self.parser.print_help()
