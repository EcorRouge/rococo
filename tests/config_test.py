"""
Tests for the Config class from the config module
"""
import os
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
