from dataclasses import dataclass, field, fields, is_dataclass
from datetime import datetime, timezone
from typing import Any, Dict
from uuid import uuid4, UUID


def default_datetime():
    return datetime.now(timezone.utc)


def get_uuid_hex(_int=None):
    return uuid4().hex if _int is None else UUID(int=_int, version=4).hex


@dataclass(kw_only=True)
class NonVersionedModel:
    """
    A base dataclass for Rococo objects that do NOT need version/audit fields.
    Mirrors VersionedModel.as_dict() logic (minus version fields).
    """

    # If you want every non-versioned document to carry an entity_id:
    entity_id: str = field(default_factory=get_uuid_hex,
                           metadata={'field_type': 'entity_id'})

    def __post_init__(self):
        # No _is_partial logic here; all fields are assumed “fully loaded.”
        pass

    def __repr__(self) -> str:
        # Show entity_id and class name
        return f"{type(self).__name__}(entity_id={self.entity_id})"

    def as_dict(self, convert_datetime_to_iso_string: bool = False,
                convert_uuids: bool = True) -> Dict[str, Any]:
        """
        Mirror VersionedModel.as_dict(), but no version/previous_version fields.
        """
        result: Dict[str, Any] = {}
        for f in fields(self):
            name = f.name
            value = getattr(self, name)
            # If it’s a nested dataclass, call its as_dict()
            if is_dataclass(value):
                result[name] = value.as_dict(
                    convert_datetime_to_iso_string, convert_uuids)
            elif isinstance(value, list):
                # If list of dataclasses or UUIDs, convert accordingly
                if value and all(isinstance(i, UUID) for i in value):
                    result[name] = [
                        str(i) if convert_uuids else i for i in value]
                elif value and all(is_dataclass(i) for i in value):
                    result[name] = [
                        i.as_dict(convert_datetime_to_iso_string, convert_uuids) for i in value]
                else:
                    result[name] = value
            elif isinstance(value, UUID):
                result[name] = str(value) if convert_uuids else value
            elif isinstance(value, datetime) and convert_datetime_to_iso_string:
                result[name] = value.isoformat()
            else:
                result[name] = value
        return result

    def get_for_db(self) -> Dict[str, Any]:
        """
        Return all fields (including entity_id) as a dict for Mongo upsert.
        """
        return self.as_dict()

    def get_for_api(self) -> Dict[str, Any]:
        """
        Strip out any internal or sensitive keys. Adjust as needed.
        """
        excluded = {'password_hash', 'raw_password', 'refresh_token_jti'}
        d = self.as_dict()
        return {k: v for k, v in d.items() if k not in excluded}

    def validate(self):
        """
        No-op for non-versioned—override in subclass if needed.
        """
        return

    def prepare_for_save(self, changed_by_id: UUID = None):
        """
        No version fields to bump; simply call validate if you want.
        """
        self.validate()
