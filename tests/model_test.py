"""
Test VersionedModel
"""
from datetime import datetime, timedelta
from rococo.models import VersionedModel
from uuid import UUID

def test_prepare_for_save():
    """
    Test model's prepare_for_save func
    """
    model = VersionedModel()
    version = model.version
    changed_by_id = 'test'

    model.prepare_for_save(changed_by_id)
    assert model.previous_version == version
    assert model.version != version
    assert model.changed_by_id == changed_by_id
    assert model.changed_on > datetime.utcnow() + timedelta(seconds=-1)


def test_as_dict():
    """
    Test converting model to dict
    """
    model = VersionedModel()

    model_as_dict = model.as_dict(True)

    assert isinstance(model_as_dict, dict)
    assert isinstance(model_as_dict['changed_on'], str)

def test_from_dict():
    """
    Test converting model to dict
    """
    model_dict = {"entity_id": UUID(int=0), "version": UUID(int=0)}

    dict_as_model = VersionedModel.from_dict(model_dict)

    assert isinstance(dict_as_model, VersionedModel)
    assert hasattr(dict_as_model, "entity_id")
    assert hasattr(dict_as_model, "version")
    assert hasattr(dict_as_model, "previous_version")
    assert hasattr(dict_as_model, "changed_by_id")
    assert hasattr(dict_as_model, "changed_on")
