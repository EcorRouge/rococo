"""
Tests for the Config class from the config module
"""
import os
import tempfile
import pytest
from rococo.config import BaseConfig


@pytest.fixture
def _env_setup():
    """
    declare an environment
    """
    os.environ["VAR_1"] = "value1"
    os.environ["VAR_2"] = "value2"
    os.environ["TO_LIST_VAR"] = "A,B,C"
    os.environ["JSON_STRING"] = '{"some_key":"some_value"}'
    yield
    del os.environ["VAR_1"]
    del os.environ["VAR_2"]
    del os.environ["TO_LIST_VAR"]
    del os.environ["JSON_STRING"]


def test_create_config(_env_setup):
    """
    Test retrieving the vars and asserting their values
    """
    config = BaseConfig()
    assert config.get_env_var("VAR_1") == "value1"
    assert config.get_env_var("VAR_2") == "value2"
    assert config.get_env_var("TO_LIST_VAR") == "A,B,C"


def test_var_to_list(_env_setup):
    """
    Test converting a comma-delimited string into a list
    """
    config = BaseConfig()
    assert config.convert_var_into_list("TO_LIST_VAR") is True
    assert config.get_env_var("TO_LIST_VAR") == ["A", "B", "C"]


def test_var_from_json(_env_setup):
    """
    Test converting a json string into a pythonic type
    """
    config = BaseConfig()
    assert config.convert_var_from_json_string("JSON_STRING") is True
    assert config.get_env_var("JSON_STRING") == {"some_key": "some_value"}


def test_project_toml_version(_env_setup):
    """
    Test reading a toml file and get it's version. Creates a temporary toml file in project root.
    """
    config = BaseConfig()
    try:
        with open("pyproject.toml", "w", encoding="UTF-8") as f:
            f.write('version = "1.0.0"')
        project_root = os.path.dirname(os.path.abspath(__file__))
        while not os.path.exists(os.path.join(project_root, 'pyproject.toml')):
            project_root = os.path.dirname(project_root)
            assert config.load_toml(project_root) is True
            assert config.get_project_version() == "1.0.0"
    finally:
        os.remove("pyproject.toml")


class TestBaseConfigGetEnvVars:
    """Tests for get_env_vars method."""

    def test_get_env_vars_returns_dict(self, _env_setup):
        """Test get_env_vars returns a dictionary."""
        config = BaseConfig()
        result = config.get_env_vars()
        
        assert isinstance(result, dict)
        assert "VAR_1" in result
        assert result["VAR_1"] == "value1"


class TestBaseConfigGetEnvVar:
    """Tests for get_env_var method."""

    def test_get_env_var_existing(self, _env_setup):
        """Test get_env_var for existing variable."""
        config = BaseConfig()
        result = config.get_env_var("VAR_1")
        assert result == "value1"

    def test_get_env_var_missing(self, _env_setup):
        """Test get_env_var for missing variable returns None."""
        config = BaseConfig()
        result = config.get_env_var("NONEXISTENT_VAR")
        assert result is None


class TestBaseConfigLoadToml:
    """Tests for load_toml method."""

    def test_load_toml_success(self):
        """Test load_toml successfully reads version."""
        with tempfile.TemporaryDirectory() as tmpdir:
            toml_path = os.path.join(tmpdir, 'pyproject.toml')
            with open(toml_path, 'w') as f:
                f.write('version = "2.5.0"')
            
            config = BaseConfig()
            result = config.load_toml(tmpdir, log_version_string=False)
            
            assert result is True
            assert config.get_project_version() == "2.5.0"

    def test_load_toml_version_not_found(self):
        """Test load_toml when version is not in file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            toml_path = os.path.join(tmpdir, 'pyproject.toml')
            with open(toml_path, 'w') as f:
                f.write('[project]\nname = "test"')
            
            config = BaseConfig()
            result = config.load_toml(tmpdir, log_version_string=False)
            
            assert result is False

    def test_load_toml_file_not_found(self):
        """Test load_toml when file doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = BaseConfig()
            result = config.load_toml(tmpdir, log_version_string=False)
            
            assert result is False


class TestBaseConfigGetProjectVersion:
    """Tests for get_project_version method."""

    def test_get_project_version_before_load(self):
        """Test get_project_version before loading toml."""
        config = BaseConfig()
        result = config.get_project_version()
        assert result is None

    def test_get_project_version_after_load(self):
        """Test get_project_version after loading toml."""
        with tempfile.TemporaryDirectory() as tmpdir:
            toml_path = os.path.join(tmpdir, 'pyproject.toml')
            with open(toml_path, 'w') as f:
                f.write('version = "3.0.0"')
            
            config = BaseConfig()
            config.load_toml(tmpdir, log_version_string=False)
            
            assert config.get_project_version() == "3.0.0"


class TestBaseConfigConvertVarIntoList:
    """Tests for convert_var_into_list method."""

    def test_convert_var_into_list_success(self, _env_setup):
        """Test convert_var_into_list with valid comma-delimited string."""
        config = BaseConfig()
        result = config.convert_var_into_list("TO_LIST_VAR")
        
        assert result is True
        assert config.get_env_var("TO_LIST_VAR") == ["A", "B", "C"]

    def test_convert_var_into_list_missing_var(self, _env_setup):
        """Test convert_var_into_list with missing variable."""
        config = BaseConfig()
        result = config.convert_var_into_list("NONEXISTENT_VAR")
        
        assert result is False


class TestBaseConfigGetVarAsList:
    """Tests for get_var_as_list method."""

    def test_get_var_as_list_success(self, _env_setup):
        """Test get_var_as_list with valid comma-delimited string."""
        config = BaseConfig()
        result = config.get_var_as_list("TO_LIST_VAR")
        
        assert result == ["A", "B", "C"]

    def test_get_var_as_list_missing_var(self, _env_setup):
        """Test get_var_as_list with missing variable."""
        config = BaseConfig()
        result = config.get_var_as_list("NONEXISTENT_VAR")
        
        assert result is None

    def test_get_var_as_list_with_spaces(self):
        """Test get_var_as_list trims whitespace."""
        os.environ["SPACED_LIST"] = "A , B , C"
        try:
            config = BaseConfig()
            result = config.get_var_as_list("SPACED_LIST")
            
            assert result == ["A", "B", "C"]
        finally:
            del os.environ["SPACED_LIST"]


class TestBaseConfigConvertVarFromJsonString:
    """Tests for convert_var_from_json_string method."""

    def test_convert_var_from_json_string_success(self, _env_setup):
        """Test convert_var_from_json_string with valid JSON."""
        config = BaseConfig()
        result = config.convert_var_from_json_string("JSON_STRING")
        
        assert result is True
        assert config.get_env_var("JSON_STRING") == {"some_key": "some_value"}

    def test_convert_var_from_json_string_invalid_json(self):
        """Test convert_var_from_json_string with invalid JSON."""
        os.environ["INVALID_JSON"] = "not valid json"
        try:
            config = BaseConfig()
            result = config.convert_var_from_json_string("INVALID_JSON")
            
            assert result is False
        finally:
            del os.environ["INVALID_JSON"]

    def test_convert_var_from_json_string_missing_var(self, _env_setup):
        """Test convert_var_from_json_string with missing variable."""
        config = BaseConfig()
        result = config.convert_var_from_json_string("NONEXISTENT_VAR")
        
        assert result is False

    def test_convert_var_from_json_string_array(self):
        """Test convert_var_from_json_string with JSON array."""
        os.environ["JSON_ARRAY"] = '[1, 2, 3]'
        try:
            config = BaseConfig()
            result = config.convert_var_from_json_string("JSON_ARRAY")
            
            assert result is True
            assert config.get_env_var("JSON_ARRAY") == [1, 2, 3]
        finally:
            del os.environ["JSON_ARRAY"]

