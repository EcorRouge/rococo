"""
base repository for rococo
"""

from typing import Any, Dict, List, Type, Union
from uuid import UUID
import json

from rococo.data.base import DbAdapter
from rococo.messaging.base import MessageAdapter
from rococo.models.versioned_model import VersionedModel


class BaseRepository:
    """
    BaseRepository class
    """
    def __init__(self, adapter: DbAdapter, model: Type[VersionedModel], message_adapter: MessageAdapter,
                 queue_name: str = 'placeholder', user_id: UUID = None):
        self.adapter = adapter
        self.message_adapter = message_adapter
        self.queue_name = queue_name
        self.model = model
        self.table_name = model.__name__.lower()
        self.user_id = user_id

    def _execute_within_context(self, func, *args, **kwargs):
        """Utility method to execute adapter methods within the context manager."""
        with self.adapter:
            return func(*args, **kwargs)
        
    def _process_data_before_save(self, data):
        """Method that is called before sending a data dictionary to adapter"""
        pass

    def _process_data_from_db(self, data):
        """Method that is called after getting data dictionary results from adapter"""
        pass

    def get_one(self, conditions: Dict[str, Any]) -> Union[VersionedModel, None]:
        """get one"""
        data = self._execute_within_context(
            self.adapter.get_one, self.table_name, conditions
        )

        self._process_data_from_db(data)

        if not data:
            return None
        return self.model.from_dict(data)

    def get_many(
        self,
        conditions: Dict[str, Any] = None,
        sort: List[tuple] = None,
        limit: int = 100,
    ) -> List[VersionedModel]:
        """get many"""
        records = self._execute_within_context(
            self.adapter.get_many, self.table_name, conditions, sort, limit
        )

        # If the adapter returned a single dictionary, wrap it in a list
        if isinstance(records, dict):
            records = [records]

        self._process_data_from_db(records)

        return [self.model.from_dict(record) for record in records]

    def save(self, instance: VersionedModel, send_message: bool = False):
        """Save func"""
        instance.prepare_for_save(changed_by_id=self.user_id)
        data = instance.as_dict(convert_datetime_to_iso_string=True)
        self._process_data_before_save(data)
        self._execute_within_context(self.adapter.save, self.table_name, data)
        if send_message:
            # This assumes that the instance is now in post-saved state with all the new DB updates
            message = json.dumps(instance.as_dict(convert_datetime_to_iso_string=True))
            self.message_adapter.send_message(self.queue_name, message)

        return instance

    def delete(self, instance: VersionedModel):
        """delete func"""
        instance.active = False
        return self.save(instance)
