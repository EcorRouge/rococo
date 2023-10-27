# base_repository.py for rococo

from typing import Any, Dict, List, Type, Union

from rococo.data.base import DbAdapter
from rococo.models.versioned_model import VersionedModel


class BaseRepository:
    def __init__(self, adapter: DbAdapter, model: Type[VersionedModel]):
        self.adapter = adapter
        self.model = model
        self.table_name = model.__name__.lower()

    def _execute_within_context(self, func, *args, **kwargs):
        """Utility method to execute adapter methods within the context manager."""
        with self.adapter:
            return func(*args, **kwargs)

    def get_one(self, conditions: Dict[str, Any]) -> Union[VersionedModel, None]:
        data = self._execute_within_context(
            self.adapter.get_one, self.table_name, conditions
        )

        if not data:
            return None
        return self.model.from_dict(data)

    def get_many(
        self,
        conditions: Dict[str, Any] = None,
        sort: List[tuple] = None,
        limit: int = 100,
    ) -> List[VersionedModel]:
        records = self._execute_within_context(
            self.adapter.get_many, self.table_name, conditions, sort, limit
        )

        # If the adapter returned a single dictionary, wrap it in a list
        if isinstance(records, dict):
            records = [records]

        return [self.model.from_dict(record) for record in records]

    def save(self, instance: VersionedModel):
        data = instance.as_dict(convert_datetime_to_iso_string=True)
        return self._execute_within_context(self.adapter.save, self.table_name, data)

    def delete(self, conditions: Dict[str, Any]) -> bool:
        return self._execute_within_context(
            self.adapter.delete, self.table_name, conditions
        )
