"""MysqlRepository class"""

from typing import Any, Dict, List, Type, Union
from uuid import UUID

from rococo.data import MySqlDbAdapter
from rococo.messaging import MessageAdapter
from rococo.models.mysql import VersionedModel
from rococo.repositories.mysql import BaseRepository


class MysqlRepository(BaseRepository):
    """MysqlRepository class"""
    def __init__( # pylint: disable=R0913
            self,
            db_adapter: MySqlDbAdapter,
            model: Type[VersionedModel],
            message_adapter: MessageAdapter,
            queue_name: str,
            user_id: UUID = None
    ):
        super().__init__(db_adapter, model, message_adapter, queue_name, user_id=user_id)
        self.model()

    def _extract_uuid_from_str(self,string):
        try:
            return UUID(string)
        except Exception: # pylint: disable=W0718
            return string

    def _process_data_from_db(self, data):
        """Method to convert data dictionary fetched from MySQL to a VersionedModel instance."""

        def _process_record(data: dict, model):
            model()
            return model.from_dict(data)

        if data is None:
            return None
        if isinstance(data, list):
            for record in data:
                _process_record(record, self.model)
        elif isinstance(data, dict):
            _process_record(data, self.model)
        else:
            raise NotImplementedError


    def get_one(self,
                conditions: Dict[str, Any]
                ) -> Union[VersionedModel, None]:
        """get one"""
        additional_fields = []
        data = self._execute_within_context(
            self.adapter.get_one, self.table_name, conditions, additional_fields=additional_fields
        )

        self.model()  # Calls __post_init__ of model to import related models and update fields.

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
        additional_fields = []
        records = self._execute_within_context(
            self.adapter.get_many,
            self.table_name,
            conditions,
            sort,
            limit,
            additional_fields=additional_fields
        )

        # If the adapter returned a single dictionary, wrap it in a list
        if isinstance(records, dict):
            records = [records]

        self.model()  # Calls __post_init__ of model to import related models and update fields.

        self._process_data_from_db(records)

        return [self.model.from_dict(record) for record in records]

