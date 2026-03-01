import json
import logging
from uuid import UUID
from typing import Any, Dict, List, Optional, Type, Tuple
from rococo.data.dynamodb import DynamoDbAdapter
from rococo.messaging import MessageAdapter
from rococo.repositories import BaseRepository
from rococo.models.versioned_model import BaseModel, VersionedModel, get_uuid_hex


class DynamoDbRepository(BaseRepository):
    """Generic DynamoDB repository for BaseModel with audit and messaging."""

    def __init__(
        self,
        db_adapter: DynamoDbAdapter,
        model: Type[BaseModel],
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
        instance: BaseModel
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
    ) -> Optional[BaseModel]:
        db_conditions = conditions.copy() if conditions else {}
        # Only add active condition for VersionedModel
        if self._is_versioned_model() and "active" not in db_conditions:
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
    ) -> List[BaseModel]:
        db_conditions = conditions.copy() if conditions else {}
        # Only add active condition for VersionedModel
        if self._is_versioned_model() and "active" not in db_conditions:
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
        instance: BaseModel,
        send_message: bool = False
    ) -> BaseModel:
        payload = self._process_data_before_save(instance)

        if self._is_versioned_model():
            ops: List[Any] = []

            if self.use_audit_table:
                previous_version = getattr(instance, 'previous_version', None)
                if previous_version and previous_version != get_uuid_hex(0):
                    audit_op = self.adapter.get_move_entity_to_audit_table_query(
                        self.table_name,
                        instance.entity_id,
                        model_cls=self.model
                    )
                    if audit_op:
                        ops.append(audit_op)

            ops.append(
                self.adapter.get_save_query(
                    self.table_name,
                    payload,
                    model_cls=self.model
                )
            )

            self._execute_within_context(lambda: self.adapter.run_transaction(ops))
            saved = payload
        else:
            saved = self._execute_within_context(
                lambda: self.adapter.upsert(self.table_name, payload, model_cls=self.model)
            )

        # Hydrate the returned fields onto our instance
        if saved:
            for k, v in saved.items():
                if hasattr(instance, k):
                    setattr(instance, k, v)

        # Send a message if requested
        if send_message:
            self.message_adapter.send_message(
                self.queue_name,
                json.dumps(instance.as_dict(
                    convert_datetime_to_iso_string=True))
            )

        return instance

    def delete(
        self,
        instance: BaseModel
    ) -> BaseModel:
        """
        Deletes a BaseModel instance from the database.
        For VersionedModel, sets its active flag to False (soft delete).
        For non-versioned models (BaseModel), performs a hard delete from the database.

        Args:
            instance (BaseModel): The BaseModel instance to delete.

        Returns:
            BaseModel: The deleted BaseModel instance.
        """
        self.logger.info(
            f"Deleting entity_id={getattr(instance, 'entity_id', 'N/A')} from {self.table_name}")

        if self._is_versioned_model():
            # Soft delete for versioned models
            instance.prepare_for_save(changed_by_id=self.user_id)
            instance.active = False

            data = instance.as_dict(
                convert_datetime_to_iso_string=True, convert_uuids=True, export_properties=self.save_calculated_fields)

            ops: List[Any] = []

            if self.use_audit_table:
                previous_version = getattr(instance, 'previous_version', None)
                if previous_version and previous_version != get_uuid_hex(0):
                    audit_op = self.adapter.get_move_entity_to_audit_table_query(
                        self.table_name,
                        data['entity_id'],
                        model_cls=self.model
                    )
                    if audit_op:
                        ops.append(audit_op)

            ops.append(
                self.adapter.get_save_query(
                    self.table_name,
                    data,
                    model_cls=self.model
                )
            )

            self._execute_within_context(lambda: self.adapter.run_transaction(ops))
            saved = data

            if saved:
                for k, v in saved.items():
                    if hasattr(instance, k):
                        setattr(instance, k, v)
        else:
            # Hard delete for non-versioned models
            with self.adapter:
                pynamo_model = self.adapter._generate_pynamo_model(self.table_name, self.model)
                try:
                    item = pynamo_model.get(instance.entity_id)
                    item.delete()
                except Exception as e:
                    self.logger.warning(f"Could not delete entity: {e}")

        return instance
