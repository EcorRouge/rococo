import json
import logging
from uuid import UUID
from typing import Any, Dict, List, Optional, Type, Tuple
from rococo.data.dynamodb import DynamoDbAdapter
from rococo.messaging import MessageAdapter
from rococo.repositories import BaseRepository
from rococo.models.versioned_model import VersionedModel, get_uuid_hex


class DynamoDbRepository(BaseRepository):
    """Generic DynamoDB repository for VersionedModel with audit and messaging."""

    def __init__(
        self,
        db_adapter: DynamoDbAdapter,
        model: Type[VersionedModel],
        message_adapter: MessageAdapter,
        queue_name: str,
        user_id: Optional[UUID] = None
    ):
        super().__init__(
            db_adapter,
            model,
            message_adapter,
            queue_name,
            user_id=user_id
        )
        self.adapter: DynamoDbAdapter = db_adapter
        self.logger = logging.getLogger(
            f"{__name__}.{self.__class__.__name__}")
        if not logging.getLogger().hasHandlers():
            logging.basicConfig(
                level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    def _process_data_before_save(
        self,
        instance: VersionedModel
    ) -> Dict[str, Any]:
        instance.prepare_for_save(changed_by_id=self.user_id)
        data = instance.as_dict(
            convert_datetime_to_iso_string=True,
            convert_uuids=True,
            export_properties=self.save_calculated_fields
        )
        return data

    def get_one(
        self,
        conditions: Dict[str, Any],
        fetch_related: List[str] = None
    ) -> Optional[VersionedModel]:
        db_conditions = conditions.copy() if conditions else {}
        if "active" not in db_conditions:
            db_conditions["active"] = True

        data = self._execute_within_context(
            lambda: self.adapter.get_one(
                table=self.table_name,
                conditions=db_conditions,
                model_cls=self.model
            )
        )

        if not data:
            return None

        self._process_data_from_db(data)
        return self.model.from_dict(data)

    def get_many(
        self,
        conditions: Dict[str, Any] = None,
        sort: List[Tuple[str, int]] = None,
        limit: int = 100,
        offset: int = 0,
        fetch_related: List[str] = None
    ) -> List[VersionedModel]:
        db_conditions = conditions.copy() if conditions else {}
        if "active" not in db_conditions:
            db_conditions["active"] = True

        records_data = self._execute_within_context(
            lambda: self.adapter.get_many(
                table=self.table_name,
                conditions=db_conditions,
                sort=sort,
                limit=limit,
                model_cls=self.model
            )
        )

        if not records_data:
            return []

        result = []
        for data in records_data:
            self._process_data_from_db(data)
            result.append(self.model.from_dict(data))
        return result

    def save(
        self,
        instance: VersionedModel,
        send_message: bool = False
    ) -> VersionedModel:
        # 1) if this isn't the very first version, move the existing latest doc into audit
        if self.use_audit_table and instance.previous_version and instance.previous_version != get_uuid_hex(0):
            self._execute_within_context(
                lambda: self.adapter.move_entity_to_audit_table(
                    self.table_name,
                    instance.entity_id,
                    model_cls=self.model
                )
            )

        # 2) Prepare & write the new version
        payload = self._process_data_before_save(instance)
        saved = self._execute_within_context(
            lambda: self.adapter.save(self.table_name, payload, model_cls=self.model)
        )

        # 3) Hydrate the returned fields onto our instance
        if saved:
            for k, v in saved.items():
                if hasattr(instance, k):
                    setattr(instance, k, v)

        # 4) Send a message if requested
        if send_message:
            self.message_adapter.send_message(
                self.queue_name,
                json.dumps(instance.as_dict(
                    convert_datetime_to_iso_string=True))
            )

        return instance

    def delete(
        self,
        instance: VersionedModel
    ) -> VersionedModel:
        self.logger.info(
            f"Deleting entity_id={getattr(instance, 'entity_id', 'N/A')} from {self.table_name}")

        instance.prepare_for_save(changed_by_id=self.user_id)
        instance.active = False

        data = instance.as_dict(
            convert_datetime_to_iso_string=True, convert_uuids=True, export_properties=self.save_calculated_fields)

        if self.use_audit_table and instance.previous_version and instance.previous_version != get_uuid_hex(0):
            self._execute_within_context(
                lambda: self.adapter.move_entity_to_audit_table(
                    self.table_name,
                    data['entity_id'],
                    model_cls=self.model
                )
            )

        saved = self._execute_within_context(
            lambda: self.adapter.save(
                self.table_name,
                data,
                model_cls=self.model
            )
        )

        if saved:
            for k, v in saved.items():
                if hasattr(instance, k):
                    setattr(instance, k, v)

        return instance
