"""
base repository for rococo
"""

from typing import Any, Dict, List, Type, Union
from uuid import UUID
import json

from rococo.data.base import DbAdapter
from rococo.messaging.base import MessageAdapter
from rococo.models import BaseVersionedModel


class BaseRepository:
    """
    BaseRepository class
    """
    def __init__(self, # pylint: disable=R0913
                 adapter: DbAdapter,
                 model: Type[BaseVersionedModel],
                 message_adapter: MessageAdapter,
                 queue_name: str = 'placeholder',
                 user_id: UUID = None):
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

    def _process_data_before_save(self, instance: BaseVersionedModel):
        """Method to convert a VersionedModel instance to data dictionary to be sent to adapter."""
        instance.prepare_for_save(changed_by_id=self.user_id)
        return instance.as_dict(convert_datetime_to_iso_string=True)

    def _process_data_from_db(self, data):
        """Method to convert a data dictionary fetched from adapter into a VersionedModel object."""
        pass # pylint: disable=W0107

    def get_one(self, conditions: Dict[str, Any]) -> Union[BaseVersionedModel, None]:
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
    ) -> List[BaseVersionedModel]:
        """get many"""
        records = self._execute_within_context(
            self.adapter.get_many, self.table_name, conditions, sort, limit
        )

        # If the adapter returned a single dictionary, wrap it in a list
        if isinstance(records, dict):
            records = [records]

        self._process_data_from_db(records)

        return [self.model.from_dict(record) for record in records]

    def save(self, instance: BaseVersionedModel, send_message: bool = False):
        """Save func"""
        data = self._process_data_before_save(instance)
        self._execute_within_context(
            self.adapter.move_entity_to_audit_table,
            self.table_name,
            instance.entity_id)
        self._execute_within_context(self.adapter.save, self.table_name, data)
        if send_message:
            # This assumes that the instance is now in post-saved state with all the new DB updates
            message = json.dumps(instance.as_dict(convert_datetime_to_iso_string=True))
            self.message_adapter.send_message(self.queue_name, message)

        return instance

    def delete(self, instance: BaseVersionedModel):
        """delete func"""
        instance.active = False
        return self.save(instance)
