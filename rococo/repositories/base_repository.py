# base_repository.py for rococo

from typing import Any, Dict, List, Type, Union

from rococo.data.base import DbAdapter
from rococo.models.versioned_model import VersionedModel


class BaseRepository:
    def __init__(self, adapter: DbAdapter, model: Type[VersionedModel]):
        self.adapter = adapter
        self.model = model
        self.table_name = model.__name__.lower()

    def get_one(self, conditions: Dict[str, Any]) -> Union[VersionedModel, None]:
        data = self.adapter.get_one(self.table_name, conditions)
        if not data:
            return None
        return self.model.from_dict(data)

    def get_many(self, conditions: Dict[str, Any] = None, sort: List[tuple] = None, 
                 limit: int = 100) -> List[VersionedModel]:
        records = self.adapter.get_many(self.table_name, conditions, sort, limit)
        return [self.model.from_dict(record) for record in records]

    def save(self, instance: VersionedModel):
        data = instance.as_dict(convert_datetime_to_iso_string=True)
        return self.adapter.save(self.table_name, data)

    def delete(self, conditions: Dict[str, Any]) -> bool:
        return self.adapter.delete(self.table_name, conditions)
