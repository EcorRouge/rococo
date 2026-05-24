import json
import logging
from uuid import UUID
from typing import Any, Dict, List, Optional, Type, Tuple, Union
from rococo.data.dynamodb import DynamoDbAdapter
from rococo.messaging import MessageAdapter
from rococo.repositories import BaseRepository
from rococo.models.versioned_model import BaseModel


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
        # Prepare the data for saving
        payload = self._process_data_before_save(instance)

        # Use appropriate save method based on model type
        if self._is_versioned_model():
            saved = self._save_versioned(payload)
        else:
            # For non-versioned models, use simple upsert
            saved = self._execute_within_context(
                lambda: self.adapter.upsert(self.table_name, payload, model_cls=self.model)
            )

        self._hydrate_instance(instance, saved)

        # Send a message if requested
        if send_message:
            self.message_adapter.send_message(
                self.queue_name,
                json.dumps(instance.as_dict(
                    convert_datetime_to_iso_string=True))
            )

        return instance

    def _save_versioned(
        self,
        payload: Dict[str, Any]
    ) -> Union[Dict[str, Any], None]:
        return self._execute_within_context(
            lambda: self.adapter.save_versioned(
                self.table_name,
                payload,
                model_cls=self.model,
                write_audit=self.use_audit_table
            )
        )

    @staticmethod
    def _hydrate_instance(
        instance: BaseModel,
        saved: Union[Dict[str, Any], None]
    ) -> None:
        if saved:
            for k, v in saved.items():
                if hasattr(instance, k):
                    setattr(instance, k, v)

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

            saved = self._save_versioned(data)
            self._hydrate_instance(instance, saved)
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
