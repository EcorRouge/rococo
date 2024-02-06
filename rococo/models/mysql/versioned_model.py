"""
VersionedModel for rococo. Base model
"""

import pkgutil
import os

from uuid import uuid4, UUID
from dataclasses import dataclass, field, fields, InitVar
from datetime import datetime
import json
from typing import Any, Dict, List
import importlib


def default_datetime():
    """
    Definition for default datetime
    """
    return datetime.utcnow()


def import_models_module(current_module, module_name):
    root_path = os.path.dirname(os.path.abspath(current_module.__file__))

    for root, dirs, _ in os.walk(root_path):
        for module in pkgutil.iter_modules([os.path.join(root, dir) for dir in dirs] + [root]):
            if module.name == module_name:
                spec = importlib.util.spec_from_file_location(module_name, os.path.join(module.module_finder.path, module_name, '__init__.py'))
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                return module


@dataclass(kw_only=True)
class VersionedModel:
    """A base class for versioned models with common (Big 6) attributes."""
    entity_id: UUID = field(default_factory=uuid4)
    version: UUID = UUID('00000000-0000-4000-8000-000000000000')
    previous_version: UUID = None
    active: bool = True
    changed_by_id: UUID = UUID('00000000-0000-4000-8000-000000000000')
    changed_on: datetime = field(default_factory=default_datetime)

    def __post_init__(self):
        # convert changed_on from datetime to isostring

        # find all fields that are datetime and convert them to isoformat
        for field in fields(self):
            if isinstance(self.__dict__[field], datetime):
                self.__dict__[field] = self.__dict__[field].isoformat()

    @classmethod
    def fields(cls) -> List[str]:
        """Get a list of field names for this model.
        
        Returns:
            List[str]: A list of field names.
        """
        return [f.name for f in fields(cls)]

    def as_dict(self, convert_datetime_to_iso_string: bool = False) -> Dict[str,Any]:
        """Convert this model to a dictionary.

        Args:
            convert_datetime_to_iso_string (bool, optional): Whether to
              convert datetime objects to ISO strings.
            Defaults to False.

        Returns:
            Dict[str, Any]: A dictionary representation of this model.
        """
        results = self.__dict__

        # convert datetime values to isoformat
        for key, value in results.items():
            if convert_datetime_to_iso_string:
                if isinstance(value, datetime):
                    results[key] = value.isoformat()
            if isinstance(value,UUID):
                results[key] = str(value)

        return results
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "VersionedModel":
        """
        Load VersionedModel from dict
        """
        filtered_data = {k: v for k, v in data.items() if k in cls.fields()}
        for k,v in filtered_data.items():
            if k in ["entity_id", "version", "previous_version", "changed_by_id"]:
                if v is not None and not isinstance(v, UUID):
                    try:
                        # Attempt to cast the string to a UUID
                        filtered_data[k] = UUID(v)
                    except ValueError:
                        # Handle the case where the string is not a valid UUID
                        print(f"'{v}' is not a valid UUID.")
        return cls(**filtered_data)

    def prepare_for_save(self, changed_by_id: UUID):
        """
        Prepare this model for saving to the database.

        Args:
            changed_by_id (str): The ID of the user making the change.
        """
        if not self.entity_id:
            self.entity_id = uuid4()
        if self.version:
            self.previous_version = self.version
        else:
            self.previous_version = UUID('00000000-0000-4000-8000-000000000000')
        self.version = uuid4()
        self.changed_on = datetime.utcnow()
        if changed_by_id is not None:
            self.changed_by_id = changed_by_id

