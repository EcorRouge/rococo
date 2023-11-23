"""
VersionedModel for rococo. Base model
"""

from uuid import uuid4, UUID
from dataclasses import dataclass, field, fields, InitVar
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

    entity_id: UUID = field(default_factory=uuid4, metadata={'field_type': 'record_id'})
    version: UUID = UUID('00000000-0000-4000-8000-000000000000')
    previous_version: UUID = None
    active: bool = True
    changed_by_id: UUID = UUID('00000000-0000-4000-8000-000000000000')
    changed_on: datetime = field(default_factory=default_datetime)

    _is_partial: InitVar[bool] = False

    def __post_init__(self, _is_partial):
        self._is_partial = _is_partial

    def __getattribute__(self, name):
        fields = [field for field in object.__getattribute__(self, 'fields')()]
        if name in fields:
            if object.__getattribute__(self, '_is_partial') == True and name != 'entity_id':
                raise AttributeError(
                    f"The object being accessed is not fetched from the database, "
                    f"and has no attributes available other than entity_id. Accessed: {name}"
                )
        return object.__getattribute__(self, name)

    def __repr__(self) -> str:
        if self._is_partial:
            return f"{self.__class__.__name__}(entity_id={self.entity_id.__repr__()}, _is_partial=True)"
        else:
            _fields = [field for field in object.__getattribute__(self, 'fields')()]
            return f"{self.__class__.__name__}(" + \
                    ', '.join([f"{f}={getattr(self, f).__repr__()}"
                                        for f in _fields]) + ')'

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
            convert_datetime_to_iso_string (bool, optional): Whether to
              convert datetime objects to ISO strings.
            Defaults to False.

        Returns:
            Dict[str, Any]: A dictionary representation of this model.
        """
        results = self.__dict__

        results = {k:v for k,v in results.items() if k in self.fields()}

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
            if k in ["entity_id","version","previous_version","changed_by_id"]:
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
