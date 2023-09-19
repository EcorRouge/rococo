from uuid import uuid4
from dataclasses import dataclass, field, fields
from datetime import datetime


@dataclass
class VersionedModel:
    entity_id: str = field(default_factory=lambda: uuid4().hex)
    version: str = field(default_factory=lambda: uuid4().hex)
    previous_version: str = field(default_factory=lambda: '00000000000000000000000000000000')
    active: bool = field(default_factory=lambda: True)
    changed_by_id: str = field(default_factory=lambda: '00000000000000000000000000000000')
    changed_on: datetime = field(default_factory=lambda: datetime.utcnow())

    def fields(self):
        return [f.name for f in fields(self)]

    def as_dict(self, convert_datetime_to_iso_string=False):
        results = self.__dict__

        if convert_datetime_to_iso_string:
            for key, value in results.items():
                if isinstance(value, datetime):
                    results[key] = value.isoformat()

        return results

    @staticmethod
    def from_dict(self, model_dict):
        for key in model_dict:
            if key in self.__dict__:
                self.__dict__[key] = model_dict[key]

    def prepare_for_save(self, changed_by_id):
        self.previous_version = self.version
        self.version = uuid4().hex
        self.changed_on = datetime.utcnow()
        self.changed_by_id = changed_by_id
