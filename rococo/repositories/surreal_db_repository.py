"""SurrealDbRepository class"""

from typing import Type, Dict, Any, Union
from uuid import UUID

from rococo.data import SurrealDbAdapter
from rococo.messaging import MessageAdapter
from rococo.models import VersionedModel
from rococo.repositories import BaseRepository


class SurrealDbRepository(BaseRepository):
    """SurrealDbRepository class"""
    def __init__(
            self,
            db_adapter: SurrealDbAdapter,
            model: Type[VersionedModel],
            message_adapter: MessageAdapter,
            queue_name: str,
            user_id: UUID = None
    ):
        super().__init__(db_adapter, model, message_adapter, queue_name, user_id=user_id)


    def _extract_uuid_from_surreal_id(self, surreal_id):
        """
        Converts a SurrealDB ID to a UUID
        Example: 'organization:⟨c87616ac-e6ca-4d3e-9177-27db7d2ebca8⟩' -> UUID('c87616ac-e6ca-4d3e-9177-27db7d2ebca8')
        """
        prefix = f"{self.table_name}:⟨"
        suffix = "⟩"
        if surreal_id.startswith(prefix) and surreal_id.endswith(suffix):
            uuid_str = surreal_id[len(prefix):-len(suffix)]
            formatted_uuid = UUID(uuid_str)
            return formatted_uuid
        else:
            raise ValueError(f"Invalid input format or no UUID found in the input string: {surreal_id}")

    def _process_data_before_save(self, data):
        """Method to rename entity_id field to id before saving."""
        data['id'] = data.pop('entity_id')

    def _process_data_from_db(self, data):
        """Method to rename and convert id field to entity_id after retrieving from database."""
        if isinstance(data, list):
            for data_dict in data:
                surreal_id = data_dict.pop('id')
                data_dict['entity_id'] = self._extract_uuid_from_surreal_id(surreal_id)
        else:
            surreal_id = data.pop('id')
            data['entity_id'] = self._extract_uuid_from_surreal_id(surreal_id)
