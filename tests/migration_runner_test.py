import os
import pytest
import logging
from unittest.mock import MagicMock, mock_open # mock_open for 'builtins.open'

# Adjust this import to match the actual location of your MigrationRunner class
from rococo.migrations.common.migration_runner import MigrationRunner

# Define canonical paths for modules if needed for specific patching strategies,
# particularly if 'get_template' or other functions are imported into MigrationRunner's namespace.
# If migration_runner.py has 'from .migration_template import get_template',
# then to patch it as used by MigrationRunner, the target is where MigrationRunner finds it.
MIGRATION_RUNNER_MODULE_PATH = "rococo.migrations.common.migration_runner"


# --- Fixtures ---
@pytest.fixture
def mock_migration_obj(mocker):
    """
    Creates a comprehensive mock migration object (like MongoMigration or an SQL equivalent)
    that MigrationRunner would use.
    """
    migration = mocker.MagicMock(name="MigrationObject")
    migration.db_adapter = mocker.MagicMock(name="DbAdapter")

    # For Mongo-like path in get_db_version
    migration.get_current_db_version = mocker.MagicMock(name="GetCurrentDbVersionMethod")

    # For SQL-like path in get_db_version (methods on db_adapter)
    migration.db_adapter.execute_query = mocker.MagicMock(name="ExecuteQueryMethod")
    migration.db_adapter.parse_db_response = mocker.MagicMock(name="ParseDbResponseMethod")

    # For context manager on db_adapter (used in get_db_version)
    migration.db_adapter.__enter__ = mocker.MagicMock(return_value=migration.db_adapter, name="DbAdapterEnter")
    migration.db_adapter.__exit__ = mocker.MagicMock(return_value=None, name="DbAdapterExit")

    # For upgrade/downgrade calls from within migration scripts
    migration.update_version_table = mocker.MagicMock(name="UpdateVersionTableMethod")
    return migration

@pytest.fixture
def runner_with_mocked_helpers(mocker, mock_migration_obj):
    """
    Creates a MigrationRunner instance with its high-level helper methods
    (get_db_version, _get_migration_scripts) already mocked on the instance.
    Useful for testing methods that *use* these helpers directly.
    """
    runner = MigrationRunner(migrations_dir="/fake/migrations_for_helpers", migration=mock_migration_obj)
    mocker.patch.object(runner, 'get_db_version', name="MockedGetDbVersionOnRunnerInstance")
    mocker.patch.object(runner, '_get_migration_scripts', name="MockedGetMigrationScriptsOnRunnerInstance")
    return runner

@pytest.fixture
def raw_runner(mock_migration_obj):
    """
    Creates a plain MigrationRunner instance without its methods being pre-mocked.
    Useful for testing methods like get_db_version or _get_migration_scripts themselves,
    where lower-level dependencies (like os.listdir or methods on mock_migration_obj) will be mocked.
    """
    return MigrationRunner(migrations_dir="/fake/migrations_for_raw_runner", migration=mock_migration_obj)


# --- Tests for _get_forward_migration_script (using runner_with_mocked_helpers) ---
class TestGetForwardMigrationScript:

    def test_no_scripts_available(self, runner_with_mocked_helpers):
        runner_with_mocked_helpers.get_db_version.return_value = "0000000001"
        runner_with_mocked_helpers._get_migration_scripts.return_value = set()
        assert runner_with_mocked_helpers._get_forward_migration_script() is None

    def test_no_suitable_script_for_current_version(self, runner_with_mocked_helpers):
        runner_with_mocked_helpers.get_db_version.return_value = "0000000001"
        runner_with_mocked_helpers._get_migration_scripts.return_value = {
            "0000000002_0000000000_another_fork.py",
            "0000000003_0000000002_future_script.py"
        }
        assert runner_with_mocked_helpers._get_forward_migration_script() is None

    def test_one_suitable_script_exists(self, runner_with_mocked_helpers):
        runner_with_mocked_helpers.get_db_version.return_value = "0000000001"
        runner_with_mocked_helpers._get_migration_scripts.return_value = {
            "0000000002_0000000001_add_users_table.py",
            "0000000003_0000000002_add_posts_table.py"
        }
        expected = "0000000002_0000000001_add_users_table"
        assert runner_with_mocked_helpers._get_forward_migration_script() == expected

    def test_multiple_suitable_scripts_picks_lowest_next_version(self, runner_with_mocked_helpers):
        runner_with_mocked_helpers.get_db_version.return_value = "0000000001"
        runner_with_mocked_helpers._get_migration_scripts.return_value = {
            "0000000003_0000000001_feature_c.py",
            "0000000002_0000000001_feature_a.py",
            "0000000004_0000000001_feature_b.py",
        }
        expected = "0000000002_0000000001_feature_a"
        assert runner_with_mocked_helpers._get_forward_migration_script() == expected

    def test_scripts_with_different_descriptions_picks_correct_version(self, runner_with_mocked_helpers):
        runner_with_mocked_helpers.get_db_version.return_value = "0000000005"
        runner_with_mocked_helpers._get_migration_scripts.return_value = {
            "0000000006_0000000005_add_index.py",
            "0000000007_0000000005_update_data.py",
        }
        expected = "0000000006_0000000005_add_index"
        assert runner_with_mocked_helpers._get_forward_migration_script() == expected

    def test_no_script_if_already_at_highest_known_step(self, runner_with_mocked_helpers):
        runner_with_mocked_helpers.get_db_version.return_value = "0000000002"
        runner_with_mocked_helpers._get_migration_scripts.return_value = {
            "0000000001_0000000000_initial.py",
            "0000000002_0000000001_step_two.py",
        }
        assert runner_with_mocked_helpers._get_forward_migration_script() is None

    def test_filename_parsing_picks_correct_numeric_version(self, runner_with_mocked_helpers):
        runner_with_mocked_helpers.get_db_version.return_value = "0000000000"
        runner_with_mocked_helpers._get_migration_scripts.return_value = {
            "0000000001_0000000000_desc_a.py",
            "0000000002_0000000000_desc_b.py",
            "0000000010_0000000000_desc_c.py",
        }
        expected = "0000000001_0000000000_desc_a"
        assert runner_with_mocked_helpers._get_forward_migration_script() == expected


# --- Tests for _get_backward_migration_script (using runner_with_mocked_helpers) ---
class TestGetBackwardMigrationScript:
    def test_no_scripts_available(self, runner_with_mocked_helpers):
        runner_with_mocked_helpers.get_db_version.return_value = "0000000001"
        runner_with_mocked_helpers._get_migration_scripts.return_value = set()
        assert runner_with_mocked_helpers._get_backward_migration_script() is None

    def test_no_suitable_script_for_current_version(self, runner_with_mocked_helpers):
        runner_with_mocked_helpers.get_db_version.return_value = "0000000002"
        runner_with_mocked_helpers._get_migration_scripts.return_value = {
            "0000000001_0000000000_revert_initial.py",
            "0000000003_0000000002_revert_future.py"
        }
        assert runner_with_mocked_helpers._get_backward_migration_script() is None

    def test_one_suitable_script_exists(self, runner_with_mocked_helpers):
        runner_with_mocked_helpers.get_db_version.return_value = "0000000002"
        runner_with_mocked_helpers._get_migration_scripts.return_value = {
            "0000000002_0000000001_revert_to_one.py",
            "0000000001_0000000000_other.py"
        }
        expected = "0000000002_0000000001_revert_to_one"
        assert runner_with_mocked_helpers._get_backward_migration_script() == expected

    def test_multiple_suitable_scripts_picks_first_found_and_warns(self, runner_with_mocked_helpers, caplog):
        runner_with_mocked_helpers.get_db_version.return_value = "0000000003"
        scripts_set = {
            "0000000003_0000000002_revert_feature_b.py",
            "0000000003_0000000000_revert_feature_a_to_zero.py",
        }
        runner_with_mocked_helpers._get_migration_scripts.return_value = scripts_set

        with caplog.at_level(logging.WARNING):
            result = runner_with_mocked_helpers._get_backward_migration_script()

        possible_results = {
            "0000000003_0000000002_revert_feature_b",
            "0000000003_0000000000_revert_feature_a_to_zero"
        }
        assert result in possible_results
        assert len(caplog.records) == 1
        assert "Multiple backward migration scripts found" in caplog.records[0].getMessage()
        assert "0000000003" in caplog.records[0].getMessage()

    def test_at_initial_version_no_backward_script(self, runner_with_mocked_helpers):
        runner_with_mocked_helpers.get_db_version.return_value = "0000000000"
        runner_with_mocked_helpers._get_migration_scripts.return_value = {
            "0000000001_0000000000_initial_forward.py"
        }
        assert runner_with_mocked_helpers._get_backward_migration_script() is None


# --- Test for _get_migration_scripts (using raw_runner) ---
def test_internal_get_migration_scripts_filters_correctly(mocker, raw_runner):
    mock_os_listdir = mocker.patch('os.listdir')
    mock_os_listdir.return_value = [
        "001_000_a.py",
        "002_001_b.PY", # Should be ignored by case-sensitive ".py"
        "__init__.py",
        "not_a_script.txt",
        "lib",
        "script.pyc",
        "__pycache__",
        "003_002_c.py"
    ]
    # MigrationRunner._get_migration_scripts uses file.endswith(".py") which is case-sensitive
    expected_scripts = {"001_000_a.py", "003_002_c.py"}
    assert raw_runner._get_migration_scripts() == expected_scripts
    mock_os_listdir.assert_called_once_with(raw_runner.migrations_dir)


# --- Tests for get_db_version (using raw_runner) ---
class TestGetDbVersion:

    def test_mongo_path_success(self, raw_runner): # raw_runner has mock_migration_obj
        raw_runner.migration.get_current_db_version.return_value = "123"
        assert raw_runner.get_db_version() == "0000000123"
        raw_runner.migration.get_current_db_version.assert_called_once()
        raw_runner.migration.db_adapter.__enter__.assert_called_once()
        raw_runner.migration.db_adapter.__exit__.assert_called_once()

    def test_mongo_path_returns_already_formatted(self, raw_runner):
        raw_runner.migration.get_current_db_version.return_value = "0000000123"
        assert raw_runner.get_db_version() == "0000000123"

    def test_mongo_path_exception_fallback(self, raw_runner, caplog):
        raw_runner.migration.get_current_db_version.side_effect = Exception("DB connection error")
        with caplog.at_level(logging.WARNING):
            assert raw_runner.get_db_version() == "0000000000"
        assert "Could not retrieve DB version" in caplog.text
        assert "DB connection error" in caplog.text

    def test_mongo_path_returns_none_fallback(self, raw_runner):
        raw_runner.migration.get_current_db_version.return_value = None
        assert raw_runner.get_db_version() == "0000000000"
        # The `db_version or '0000000000'` handles None before int() conversion

    def test_sql_path_success(self, mocker, raw_runner):
        # Make hasattr(raw_runner.migration, 'get_current_db_version') return False
        # For a MagicMock, if the attribute was added by the fixture, we can del it.
        # If it's not defined, hasattr will be false. Our fixture adds it.
        del raw_runner.migration.get_current_db_version

        raw_runner.migration.db_adapter.execute_query.return_value = "dummy_sql_response"
        raw_runner.migration.db_adapter.parse_db_response.return_value = [{'version': '789'}]

        assert raw_runner.get_db_version() == "0000000789"
        raw_runner.migration.db_adapter.execute_query.assert_called_once_with("SELECT version from db_version;")
        raw_runner.migration.db_adapter.parse_db_response.assert_called_once_with("dummy_sql_response")

    def test_sql_path_empty_results(self, raw_runner):
        del raw_runner.migration.get_current_db_version
        raw_runner.migration.db_adapter.execute_query.return_value = "dummy_sql_response"
        raw_runner.migration.db_adapter.parse_db_response.return_value = []
        assert raw_runner.get_db_version() == "0000000000"

    def test_sql_path_no_version_key(self, raw_runner):
        del raw_runner.migration.get_current_db_version
        raw_runner.migration.db_adapter.execute_query.return_value = "dummy_sql_response"
        raw_runner.migration.db_adapter.parse_db_response.return_value = [{'other_key': 'abc'}]
        assert raw_runner.get_db_version() == "0000000000"
        
    def test_sql_path_result_is_dict(self, raw_runner): # For SurrealDB like response
        del raw_runner.migration.get_current_db_version
        raw_runner.migration.db_adapter.execute_query.return_value = "dummy_sql_response"
        raw_runner.migration.db_adapter.parse_db_response.return_value = {'version': '111'} # dict instead of list
        assert raw_runner.get_db_version() == "0000000111"


    def test_non_numeric_version_string_warning_and_passthrough(self, raw_runner, caplog):
        raw_runner.migration.get_current_db_version.return_value = "abc_123_def"
        with caplog.at_level(logging.WARNING):
            assert raw_runner.get_db_version() == "abc_123_def"
        assert "DB version 'abc_123_def' is not a simple integer string." in caplog.text


# --- Tests for create_migration_file (using raw_runner) ---
class TestCreateMigrationFile:
    # Use a temporary directory for these tests
    @pytest.fixture
    def temp_migrations_dir(self, tmp_path):
        # tmp_path is a pytest fixture providing a temporary directory path object
        return tmp_path

    def test_success(self, mocker, raw_runner, temp_migrations_dir, capsys):
        raw_runner.migrations_dir = str(temp_migrations_dir) # Use the temp path

        mocker.patch.object(raw_runner, 'get_db_version', return_value="0000000001")
        mocker.patch('builtins.input', return_value="add_new_feature")
        mock_file_open = mocker.patch('builtins.open', new_callable=mock_open)

        expected_template_content = "revision = \"0000000002\"\ndown_revision = \"0000000001\"..." # Abridged
        # Patch get_template within the migration_runner's module scope
        mocker.patch(f'{MIGRATION_RUNNER_MODULE_PATH}.get_template', return_value=expected_template_content)

        raw_runner.create_migration_file()

        expected_filename = "0000000002_0000000001_add_new_feature.py"
        expected_filepath = os.path.join(str(temp_migrations_dir), expected_filename)

        mock_file_open.assert_called_once_with(expected_filepath, 'w')
        mock_file_open().write.assert_called_once_with(expected_template_content)
        
        captured = capsys.readouterr()
        assert f"Created new migration file at {expected_filepath}" in captured.out


    def test_non_integer_db_version(self, mocker, raw_runner, temp_migrations_dir, caplog, capsys):
        raw_runner.migrations_dir = str(temp_migrations_dir)
        mocker.patch.object(raw_runner, 'get_db_version', return_value="not_an_int_version")

        with caplog.at_level(logging.ERROR):
            raw_runner.create_migration_file()

        assert "Cannot create new migration. Current DB version 'not_an_int_version' is not a parseable integer." in caplog.text
        captured = capsys.readouterr()
        assert "Error: DB version 'not_an_int_version' is not a valid integer." in captured.out

    def test_io_error_on_file_write(self, mocker, raw_runner, temp_migrations_dir, caplog, capsys):
        raw_runner.migrations_dir = str(temp_migrations_dir)
        mocker.patch.object(raw_runner, 'get_db_version', return_value="0000000001")
        mocker.patch('builtins.input', return_value="test_io_error")
        mocker.patch(f'{MIGRATION_RUNNER_MODULE_PATH}.get_template', return_value="template_content")
        mocker.patch('builtins.open', side_effect=IOError("Disk full"))

        with caplog.at_level(logging.ERROR):
            raw_runner.create_migration_file()

        assert "Error creating migration file" in caplog.text
        assert "Disk full" in caplog.text
        captured = capsys.readouterr()
        assert "Error creating migration file: Disk full" in captured.out


# --- Tests for run_forward_migration_script (using raw_runner) ---
class TestRunForwardMigrationScript:

    def test_no_script_found_initially(self, raw_runner, mocker, caplog):
        initial_version = "0000000000"
        mocker.patch.object(raw_runner, '_get_forward_migration_script', return_value=None)
        # Simulate get_db_version being called at the end of the run.
        # The get_db_version inside _get_forward_migration_script is implicitly part of the mock above.
        # The one at the end of run_forward_migration_script needs to be handled.
        mocker.patch.object(raw_runner, 'get_db_version', return_value=initial_version)


        with caplog.at_level(logging.INFO):
            raw_runner.run_forward_migration_script(initial_db_version_for_run=initial_version)

        raw_runner._get_forward_migration_script.assert_called_once() # Called once to find no scripts
        # get_db_version is called twice in this path: once by the mocked _get_forward_migration_script (implicitly)
        # and once explicitly at the end of run_forward_migration_script if no script is found.
        assert raw_runner.get_db_version.call_count == 1 # Only the explicit call at the end
        assert f"No forward migration scripts found for current DB version: {initial_version}" in caplog.text
        assert f"Latest DB version: {initial_version}" in caplog.text

    def test_run_one_script_successfully(self, raw_runner, mocker, caplog):
        initial_version = "0000000000"
        script_name_no_ext = "0000000001_0000000000_first_migration"
        next_version = "0000000001"

        # _get_forward_migration_script: 1st call returns script, 2nd call returns None (to stop recursion)
        mocker.patch.object(raw_runner, '_get_forward_migration_script', side_effect=[script_name_no_ext, None])
        
        # get_db_version is called by _get_forward_migration_script (implicitly mocked for the side_effect to work)
        # and at the end of run_forward_migration_script.
        # The version *after* the script runs should be next_version.
        mocker.patch.object(raw_runner, 'get_db_version', return_value=next_version) # For the final log

        mock_script_module = MagicMock()
        mock_import_module = mocker.patch('importlib.import_module', return_value=mock_script_module)

        with caplog.at_level(logging.INFO):
            raw_runner.run_forward_migration_script(initial_db_version_for_run=initial_version)

        assert mock_import_module.call_count == 1
        mock_import_module.assert_called_with(script_name_no_ext)
        mock_script_module.upgrade.assert_called_once_with(raw_runner.migration)
        assert f"Running forward migration: {script_name_no_ext}.py" in caplog.text
        assert "All pending forward migrations complete!" in caplog.text # After recursion stops
        assert f"Latest DB version: {next_version}" in caplog.text
        assert raw_runner._get_forward_migration_script.call_count == 2 # Called twice

    def test_exception_during_script_upgrade(self, raw_runner, mocker, caplog, capsys):
        initial_version = "0000000000" # Does not change due to error
        script_name_no_ext = "0000000001_0000000000_error_migration"

        mocker.patch.object(raw_runner, '_get_forward_migration_script', return_value=script_name_no_ext)
        # get_db_version will be called by the mocked _get_forward_migration_script and in the error message
        mocker.patch.object(raw_runner, 'get_db_version', return_value=initial_version)

        mock_script_module = MagicMock()
        mock_script_module.upgrade.side_effect = Exception("Upgrade failed!")
        mocker.patch('importlib.import_module', return_value=mock_script_module)

        with caplog.at_level(logging.ERROR):
            raw_runner.run_forward_migration_script(initial_db_version_for_run=initial_version)

        assert f"Error during forward migration {script_name_no_ext}.py: Upgrade failed!" in caplog.text
        captured = capsys.readouterr()
        assert f"Error applying migration {script_name_no_ext}.py. Halting." in captured.out
        assert f"Current DB version after error: {initial_version}" in captured.out


# --- Tests for run_backward_migration_script (using raw_runner) ---
class TestRunBackwardMigrationScript:

    def test_no_script_found(self, raw_runner, mocker, caplog):
        current_version = "0000000001"
        mocker.patch.object(raw_runner, '_get_backward_migration_script', return_value=None)
        # get_db_version is called by the mocked _get_backward_migration_script and for the log.
        mocker.patch.object(raw_runner, 'get_db_version', return_value=current_version)

        with caplog.at_level(logging.INFO):
            raw_runner.run_backward_migration_script()

        raw_runner._get_backward_migration_script.assert_called_once()
        # get_db_version is called by _get_backward_migration_script (implicitly) and then for the "No backward..." log.
        assert raw_runner.get_db_version.call_count == 1 # only for the log message
        assert f"No backward migration script found for current DB version: {current_version}" in caplog.text

    def test_run_one_script_successfully(self, raw_runner, mocker, caplog):
        version_after_downgrade = "0000000000"
        script_name_no_ext = "0000000001_0000000000_revert_first"

        mocker.patch.object(raw_runner, '_get_backward_migration_script', return_value=script_name_no_ext)
        # get_db_version: by _get_backward (implicitly), then twice at the end for logging.
        mocker.patch.object(raw_runner, 'get_db_version', side_effect=[version_after_downgrade, version_after_downgrade])


        mock_script_module = MagicMock()
        mock_import_module = mocker.patch('importlib.import_module', return_value=mock_script_module)

        with caplog.at_level(logging.INFO):
            raw_runner.run_backward_migration_script()

        mock_import_module.assert_called_once_with(script_name_no_ext)
        mock_script_module.downgrade.assert_called_once_with(raw_runner.migration)
        assert f"Running backward migration: {script_name_no_ext}.py" in caplog.text
        assert "Backward migration script complete." in caplog.text
        assert f"DB version is now: {version_after_downgrade}" in caplog.text
        assert raw_runner.get_db_version.call_count == 2 # For the two log messages at the end

    def test_exception_during_script_downgrade(self, raw_runner, mocker, caplog, capsys):
        current_version = "0000000001" # Version remains unchanged due to error
        script_name_no_ext = "0000000001_0000000000_error_downgrade"

        mocker.patch.object(raw_runner, '_get_backward_migration_script', return_value=script_name_no_ext)
        mocker.patch.object(raw_runner, 'get_db_version', return_value=current_version) # For error log

        mock_script_module = MagicMock()
        mock_script_module.downgrade.side_effect = Exception("Downgrade failed miserably!")
        mocker.patch('importlib.import_module', return_value=mock_script_module)

        with caplog.at_level(logging.ERROR):
            raw_runner.run_backward_migration_script()

        assert f"Error during backward migration {script_name_no_ext}.py: Downgrade failed miserably!" in caplog.text
        captured = capsys.readouterr()
        assert f"Error applying downgrade migration {script_name_no_ext}.py. Halting." in captured.out
        assert f"Current DB version after error: {current_version}" in captured.out