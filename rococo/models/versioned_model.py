"""
VersionedModel for rococo. Base model
"""

from uuid import uuid4
from dataclasses import dataclass, field, fields
from datetime import datetime
from typing import Any, Dict, List


def default_datetime():
    """
    Definition for default datetime
    """
    return datetime.utcnow()


@dataclass(kw_only=True)
class VersionedModel:
    """A base class for versioned models with common (Big 6) attributes."""

    entity_id: str = uuid4().hex
    version: str = uuid4().hex
    previous_version: str = '00000000000000000000000000000000'
    active: bool = True
    changed_by_id: str = '00000000000000000000000000000000'
    changed_on: datetime = field(default_factory=default_datetime)

    @classmethod
    def fields(cls) -> List[str]:
        """Get a list of field names for this model.
        
        Returns:
            List[str]: A list of field names.
        """
        return [f.name for f in fields(cls)]

    def as_dict(self, convert_datetime_to_iso_string: bool = False) -> Dict[str, Any]:
        """Convert this model to a dictionary.

        Args:
            convert_datetime_to_iso_string (bool, optional): Whether to convert datetime objects to ISO strings.
            Defaults to False.

        Returns:
            Dict[str, Any]: A dictionary representation of this model.
        """
        results = self.__dict__

        if convert_datetime_to_iso_string:
            for key, value in results.items():
                if isinstance(value, datetime):
                    results[key] = value.isoformat()

        return results

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "VersionedModel":
        """
        Load VersionedModel from dict
        """
        expected_keys = cls.__annotations__.keys()
        filtered_data = {k: v for k, v in data.items() if k in expected_keys}
        return cls(**filtered_data)

    def prepare_for_save(self, changed_by_id: str):
        """
        Prepare this model for saving to the database.

        Args:
            changed_by_id (str): The ID of the user making the change.
        """
        self.previous_version = self.version
        self.version = uuid4().hex
        self.changed_on = datetime.utcnow()
        self.changed_by_id = changed_by_id
