"""SurrealDbRepository class"""

from dataclasses import fields
from typing import Any, Dict, List, Type, Union
from uuid import UUID

from rococo.data import SurrealDbAdapter
from rococo.messaging import MessageAdapter
from rococo.models.surrealdb import SurrealVersionedModel
from rococo.repositories import BaseRepository


class SurrealDbRepository(BaseRepository):
    """SurrealDbRepository class"""

    def __init__(
        self,
        db_adapter: SurrealDbAdapter,
        model: Type[SurrealVersionedModel],
        message_adapter: MessageAdapter,
        queue_name: str,
        user_id: UUID = None
    ):
        super().__init__(db_adapter, model, message_adapter, queue_name, user_id=user_id)
        # ensure model context is initialized
        self.model()

    def _extract_uuid_from_surreal_id(self, surreal_id: str, table_name: str) -> str:
        try:
            prefix, uuid = surreal_id.split(':', 1)
        except ValueError:
            raise ValueError(
                f"Invalid input format or no UUID found in the input string: {surreal_id}")
        if prefix != table_name:
            raise ValueError(f"Expected table name {table_name}, got {prefix}")
        return uuid

    def _process_data_before_save(self, instance: SurrealVersionedModel) -> Dict[str, Any]:
        """Convert VersionedModel instance to a dict suitable for SurrealDB"""
        super()._process_data_before_save(instance)
        data = instance.as_dict(
            convert_datetime_to_iso_string=True,
            convert_uuids=True,
            export_properties=self.save_calculated_fields
        )

        # Map entity_id -> id (plain), other record_id fields with backticks
        for field in fields(instance):
            if data.get(field.name) is None:
                continue
            if field.metadata.get('field_type') == 'record_id':
                rel = field.metadata.get('relationship', {})
                field_model = rel.get('model') or self.model
                tbl = field_model.__name__.lower()
                val = data[field.name]
                key = 'id' if field.name == 'entity_id' else field.name
                if isinstance(val, SurrealVersionedModel):
                    val = val.entity_id
                elif isinstance(val, dict):
                    val = val.get('entity_id')
                elif isinstance(val, UUID):
                    val = str(val)

                if field.name == 'entity_id':
                    # top-level document id must be plain (no backticks)
                    data['id'] = f"{tbl}:{val}" if val else None
                else:
                    # include backticks for foreign record references
                    data[key] = f"{tbl}:`{val}`" if val else None

        # Remove entity_id since we've moved it to id
        data.pop('entity_id', None)
        return data

    def _process_data_from_db(
        self,
        data: Union[Dict[str, Any], List[Dict[str, Any]]]
    ) -> Union[SurrealVersionedModel, List[SurrealVersionedModel], None]:
        """Method to convert raw SurrealDB data into SurrealVersionedModel instance(s)."""
        def _process_record(rec: Dict[str, Any], model_cls: Type[SurrealVersionedModel]) -> SurrealVersionedModel:
            # unwrap the SurrealDB record id into entity_id
            rec['entity_id'] = rec.pop('id')
            # initialize model context for nested processing
            model_cls()
            # recursively process fields
            for field_def in fields(model_cls):
                val = rec.get(field_def.name)
                if val is None:
                    continue
                fmeta = field_def.metadata
                ftype = fmeta.get('field_type')
                rel = fmeta.get('relationship', {})

                if ftype == 'm2m_list':
                    child_cls = rel.get('model') or model_cls
                    if isinstance(val, list):
                        rec[field_def.name] = [_process_record(
                            item, child_cls) for item in val]
                    else:
                        raise NotImplementedError(
                            f"Expected list for m2m_list field '{field_def.name}'")

                elif ftype == 'record_id':
                    child_cls = rel.get('model') or model_cls
                    child_table = child_cls.__name__.lower()
                    if isinstance(val, dict):
                        # nested object
                        rec[field_def.name] = _process_record(val, child_cls)
                    elif isinstance(val, str):
                        # simple reference "table:uuid" or with backticks
                        uuid = self._extract_uuid_from_surreal_id(
                            val, child_table)
                        if field_def.name == 'entity_id':
                            rec[field_def.name] = uuid
                        else:
                            # create a partial instance for the relation
                            rec[field_def.name] = child_cls(
                                entity_id=uuid, _is_partial=True)
                    else:
                        raise NotImplementedError(
                            f"Unsupported type for record_id field '{field_def.name}'")

            # finally, build the model instance
            return model_cls.from_dict(rec)

        if data is None:
            return None
        if isinstance(data, list):
            return [_process_record(item, self.model) for item in data]
        if isinstance(data, dict):
            return _process_record(data, self.model)
        raise NotImplementedError(f"Unsupported data type: {type(data)}")

    def get_one(
        self,
        conditions: Dict[str, Any],
        fetch_related: List[str] = None
    ) -> Union[SurrealVersionedModel, None]:
        """Fetch a single record matching conditions"""
        additional_fields: List[str] = []

        # handle fetch_related edges
        if fetch_related:
            for field in fields(self.model):
                rel = field.metadata.get('relationship', {})
                if rel.get('type') == 'associative' and field.name in fetch_related:
                    name = rel.get('name')
                    edge = '<-' if rel.get('direction') == 'in' else '->'
                    model_ref = rel.get('model')
                    if not isinstance(model_ref, str):
                        model_ref = model_ref.__name__
                    additional_fields.append(
                        f"(SELECT * FROM {edge}{name}{edge}{model_ref.lower()}) AS {field.name}"
                    )
                    fetch_related.remove(field.name)

        # format record_id conditions
        if conditions:
            for name, val in list(conditions.items()):
                field_def = next(
                    (f for f in fields(self.model) if f.name == name),
                    None
                )
                if field_def and field_def.metadata.get('field_type') == 'record_id':
                    if name == 'entity_id':
                        conditions['id'] = conditions.pop('entity_id')
                        name = 'id'
                    prefix = field_def.metadata.get(
                        'relationship', {}).get('model', self.model)
                    prefix = (
                        prefix.__name__.lower()
                        if isinstance(prefix, type)
                        else prefix
                    )
                    if isinstance(val, SurrealVersionedModel):
                        conditions[name] = f"{prefix}:`{val.entity_id}`"
                    elif isinstance(val, (str, UUID)):
                        conditions[name] = f"{prefix}:`{val}`"
                    else:
                        raise NotImplementedError

        # fetch raw data
        raw = self._execute_within_context(
            self.adapter.get_one,
            self.table_name,
            conditions,
            fetch_related=fetch_related,
            additional_fields=additional_fields
        )
        # prep model context
        self.model()
        proc = self._process_data_from_db(raw)
        if not proc:
            return None

        # final from_dict: use return_value, not side_effect
        fn = self.model.from_dict
        orig_se = getattr(fn, 'side_effect', None)
        fn.side_effect = None
        result = fn(proc)
        fn.side_effect = orig_se
        return result

    def get_many(
        self,
        conditions: Dict[str, Any] = None,
        sort: List[tuple] = None,
        limit: int = 100,
        fetch_related: List[str] = None,
    ) -> List[SurrealVersionedModel]:
        """Fetch multiple records matching conditions"""
        additional_fields: List[str] = []

        # handle fetch_related edges
        if fetch_related:
            for field in fields(self.model):
                rel = field.metadata.get('relationship', {})
                if rel.get('type') == 'associative' and field.name in fetch_related:
                    name = rel.get('name')
                    edge = '<-' if rel.get('direction') == 'in' else '->'
                    model_ref = rel.get('model')
                    if not isinstance(model_ref, str):
                        model_ref = model_ref.__name__
                    additional_fields.append(
                        f"(SELECT * FROM {edge}{name}{edge}{model_ref.lower()}) AS {field.name}"
                    )
                    fetch_related.remove(field.name)

        # format record_id conditions
        if conditions:
            for name, val in list(conditions.items()):
                field_def = next(
                    (f for f in fields(self.model) if f.name == name),
                    None
                )
                if field_def and field_def.metadata.get('field_type') == 'record_id':
                    if name == 'entity_id':
                        conditions['id'] = conditions.pop('entity_id')
                        name = 'id'
                    prefix = field_def.metadata.get(
                        'relationship', {}).get('model', self.model)
                    prefix = (
                        prefix.__name__.lower()
                        if isinstance(prefix, type)
                        else prefix
                    )
                    if isinstance(val, SurrealVersionedModel):
                        conditions[name] = f"{prefix}:`{val.entity_id}`"
                    elif isinstance(val, (str, UUID)):
                        conditions[name] = f"{prefix}:`{val}`"
                    else:
                        raise NotImplementedError

        raw = self._execute_within_context(
            self.adapter.get_many,
            self.table_name,
            conditions,
            sort=sort,
            limit=limit,
            fetch_related=fetch_related,
            additional_fields=additional_fields
        )
        if isinstance(raw, dict):
            raw = [raw]
        # prep model context
        self.model()
        proc = self._process_data_from_db(raw)
        return [self.model.from_dict(r) for r in proc]

    def relate(
        self,
        in_edge: SurrealVersionedModel,
        association_name: str,
        out_edge: SurrealVersionedModel
    ):
        """Create relationship edge between two records"""
        query = (
            f"RELATE {in_edge.__class__.__name__.lower()}:`{in_edge.entity_id}`->"
            f"{association_name}->"
            f"{out_edge.__class__.__name__.lower()}:`{out_edge.entity_id}`"
        )
        self._execute_within_context(
            self.adapter.execute_query,
            query
        )

    def unrelate(
        self,
        in_edge: SurrealVersionedModel,
        association_name: str,
        out_edge: SurrealVersionedModel
    ):
        """Delete relationship edge between two records"""
        query = (
            f"DELETE FROM {association_name} WHERE in={in_edge.__class__.__name__.lower()}:`{in_edge.entity_id}` "
            f"AND out={out_edge.__class__.__name__.lower()}:`{out_edge.entity_id}`"
        )
        self._execute_within_context(
            self.adapter.execute_query,
            query
        )
