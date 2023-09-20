from uuid import uuid4
from dataclasses import dataclass, field, fields
from datetime import datetime
from typing import Any, Dict, List


@dataclass(kw_only=True)
class VersionedModel:
    """A base class for versioned models with common (Big 6) attributes."""

    entity_id: str = uuid4().hex
    version: str = uuid4().hex
    previous_version: str = '00000000000000000000000000000000'
    active: bool = True
    changed_by_id: str = '00000000000000000000000000000000'
    changed_on: datetime = field(default_factory=lambda: datetime.utcnow())

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
            convert_datetime_to_iso_string (bool, optional): Whether to convert datetime objects to ISO strings. Defaults to False.

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
    def from_dict(cls, model_dict: dict):
        """Create a model from a dictionary.

        Args:
            model_dict (dict): A dictionary representation of a model.

        Returns:
            VersionedModel: A model object.
        """
        for key in model_dict:
            if key in cls.__dict__:
                cls.__dict__[key] = model_dict[key]

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
