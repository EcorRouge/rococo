"""
Config class that allows to load from a .toml file, and/or use a .env file.
"""
import os
import re
import json
from abc import abstractmethod
from typing import Optional
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


class BaseConfig():
    """
    Config class that allows to load from a .toml file, and/or use a .env file.
    """
    def __init__(self):
        load_dotenv()
        self.project_version = None
        # Get all environment variables and store them in a dictionary
        self.env_vars = {key: os.getenv(key) for key in os.environ}

    def get_env_vars(self) -> dict:
        """
        Return the dictionary containing all environment variables
        """
        return self.env_vars

    def get_env_var(self, var_name: str):
        """
        Retrieve the value of the specified environment variable
        Args:
            var_name (str) : Name of the env var to fetch 
        """
        if var_name in self.env_vars.keys():
            return self.env_vars[var_name]
        else:
            logger.warning("Variable %s not found.", var_name)
            return None

    def load_toml(self, toml_folder_dir: str, log_version_string: bool=True) -> bool:
        """
        Loads the toml file for the project
        Args:
            toml_folder_dir (str) : Path to the folder where the toml file exists.
        """
        pyproject_path = os.path.join(toml_folder_dir, 'pyproject.toml')

        # Read pyproject.toml and extract the version using regular expression
        try:
            with open(pyproject_path, 'r', encoding='UTF-8') as file:
                pyproject_content = file.read()
                version_match = re.search(r'version\s*=\s*[\'"]([^\'"]+)[\'"]', pyproject_content)
                if version_match:
                    version = version_match.group(1)
                    if log_version_string:
                        logger.info('Project Version: %s', version)
                    self.project_version = version
                    return True
                else:
                    logger.error('Version not found in pyproject.toml.')
                    return False
        except FileNotFoundError:
            logger.error('pyproject.toml not found for toml_folder_dir = %s', toml_folder_dir)
            return False

    def get_project_version(self) -> Optional[str]:
        """
        Returns the project version from the toml file
        """
        return self.project_version

    def convert_var_into_list(self, var_name: str) -> bool:
        """
        Converts a comma-delimited var into a list
        """
        if var_name in self.env_vars.keys():
            try:
                self.env_vars[var_name] = [env_var.strip() for env_var in self.env_vars[var_name].split(",")]
                return True
            except ValueError:
                logger.error(
                    "Error: Invalid input format. Please provide a comma-delimited string.")
                return False
        logger.warning("Warning: var %s not found.", var_name)
        return False

    def get_var_as_list(self, var_name: str) -> bool:
        """
        Returns a comma-delimited var as list
        """
        if var_name in self.env_vars.keys():
            try:
                return [env_var.strip() for env_var in self.env_vars[var_name].split(",")]
            except ValueError:
                logger.error(
                    "Error: Invalid input format. Please provide a comma-delimited string.")
        logger.warning("Warning: var %s not found.", var_name)
        return None


    def convert_var_from_json_string(self, var_name: str) -> bool:
        """
        Converts a json string into a pythonic type
        """
        if var_name in self.env_vars.keys():
            try:
                self.env_vars[var_name] = json.loads(self.env_vars[var_name])
                return True
            except ValueError:
                logger.error("Error: Invalid input format. Please provide a proper json string.")
                return False
        logger.warning("Warning: var %s not found.", var_name)
        return False

    @abstractmethod
    def validate_env_vars(self):
        """
        Abstract method for validation of the env vars.
        """
