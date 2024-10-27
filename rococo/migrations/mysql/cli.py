import argparse
import os
import sys
from collections import OrderedDict
from dotenv import dotenv_values


def check_required_env_vars(required_vars):
    """ Check if required environment variables exist in the current environment. """
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    return missing_vars


def main():
    parser = argparse.ArgumentParser(description="CLI interface for my Rococo migrations module.")
    parser.add_argument('--migrations-dir', type=str, help="Path to migrations directory of your project.", default=None)
    parser.add_argument('--env-files', type=str, nargs='+', help="Path to environment files.", default=[])

    # Add subcommands using subparsers
    subparsers = parser.add_subparsers(dest="command", help="Subcommand to run")
    new_parser = subparsers.add_parser('new', help="Create new migration file.")
    rf_parser  = subparsers.add_parser('rf', help="Run forward migration")
    rb_parser = subparsers.add_parser('rb', help="Run backward migration")
    version_parser = subparsers.add_parser('version', help="Get DB version")

    args = parser.parse_args()

    # Check for migrations directory
    migrations_dir = None
    if args.migrations_dir and os.path.exists(args.migrations_dir) and os.path.isdir(args.migrations_dir):
        migrations_dir = args.migrations_dir
    elif not args.migrations_dir:
        dirs_to_check = ['flask/app/migrations', 'api/app/migrations', 'app/migrations']
        for _dir in dirs_to_check:
            if os.path.exists(_dir) and os.path.isdir(_dir):
                migrations_dir = _dir
    
    if not migrations_dir:
        parser.error("No migrations directory found at the specified path.")
        return

    # Required environment variables
    required_env_vars = ['MYSQL_HOST', 'MYSQL_PORT', 'MYSQL_USER', 'MYSQL_PASSWORD', 'MYSQL_DATABASE']
    
    # Check if env files are provided
    if not args.env_files:
        # First, check environment variables in the current environment
        missing_vars = check_required_env_vars(required_env_vars)
        
        if missing_vars:
            # If any required env vars are missing, check .env files
            print(f"Missing environment variables: {', '.join(missing_vars)}. Trying to load from .env files...")

            if os.path.exists('.env.secrets') and os.path.isfile('.env.secrets'):
                secrets_env = dotenv_values('.env.secrets')
                try:
                    app_env_name = secrets_env['APP_ENV']
                except KeyError:
                    app_env_name = None
                
                if app_env_name and os.path.exists(f'{app_env_name}.env') and os.path.isfile(f'{app_env_name}.env'):
                    app_env = dotenv_values(f'{app_env_name}.env')
                    merged_env = OrderedDict(list(secrets_env.items()) + list(app_env.items()))
                else:
                    parser.error('APP_ENV not found in .env.secrets file or <APP_ENV>.env file does not exist.')
                    return
            else:
                parser.error('.env.secrets not found in project directory. Specify env file(s) with --env-files argument.')
                return
        else:
            # If all required environment variables are present
            merged_env = {var: os.getenv(var) for var in required_env_vars}
    else:
        # Load env files if specified
        merged_env = []
        for env_file in args.env_files:
            if os.path.exists(env_file) and os.path.isfile(env_file):
                merged_env += list(dotenv_values(env_file).items())
            else:
                parser.error(f"{env_file} file specified in --env-files argument not found.")
                return
        merged_env = OrderedDict(merged_env)

    from rococo.data.mysql import MySqlAdapter
    from .run import MigrationRunner
    try:
        db_adapter = MySqlAdapter(
            host=merged_env['MYSQL_HOST'],
            port=int(merged_env['MYSQL_PORT']),
            user=merged_env['MYSQL_USER'],
            password=merged_env['MYSQL_PASSWORD'],
            database=merged_env['MYSQL_DATABASE']
        )
    except KeyError as e:
        parser.error(f'{e.args[0]} key not found in the environment variables or env files.')
        return

    sys.path.append(migrations_dir)
    runner = MigrationRunner(migrations_dir, db_adapter)
    db_version = runner.get_db_version()
    print(f'Current db version: {db_version}')
    if args.command == 'new':
        runner.create_migration_file()
    elif args.command == 'rf':
        runner.run_forward_migration_script(db_version)
    elif args.command == 'rb':
        runner.run_backward_migration_script()
    elif args.command == 'version':
        pass
    else:
        parser.print_help()
