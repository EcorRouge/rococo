import json
from typing import Type

from rococo.data import SurrealDbAdapter
from rococo.messaging import MessageAdapter
from rococo.models import VersionedModel
from rococo.repositories import BaseRepository


class SurrealDbRepository(BaseRepository):
    def __init__(
            self,
            db_adapter: SurrealDbAdapter,
            model: Type[VersionedModel],
            message_adapter: MessageAdapter,
            queue_name: str,
    ):
        super().__init__(db_adapter, model, message_adapter, queue_name)

    def save(self, instance: VersionedModel, send_message: bool = True):
        self._execute_within_context(
            self.adapter.save, self.table_name, instance.as_dict()
        )

        # After save, if the instance is updated with post-saved state, serialize and send via message adapter
        if send_message:
            # This assumes that the instance is now in post-saved state with all the new DB updates
            message = json.dumps(instance.as_dict(convert_datetime_to_iso_string=True))
            self.message_adapter.send_message(self.queue_name, message)

        return instance
