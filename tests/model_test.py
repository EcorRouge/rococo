"""
Test VersionedModel
"""
from datetime import datetime, timedelta
from uuid import UUID
from dataclasses import dataclass
from rococo.models import VersionedModel

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

    model.attribute_that_should_not_exist = "SomeValue"

    model_as_dict = model.as_dict(True)

    assert isinstance(model_as_dict, dict)
    assert isinstance(model_as_dict['changed_on'], str)
    assert 'attribute_that_should_not_exist' not in model_as_dict

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

def test_sublass_from_dict():
    """
    Test converting subclassed model to dict
    """

    @dataclass
    class TestModel(VersionedModel):
        """TestModel for VersionedModel"""
        test_attribute: int = 0

    model_dict = {"entity_id": UUID(int=0), "version": UUID(int=0), "test_attribute": 5}
    dict_as_model = TestModel.from_dict(model_dict)

    assert isinstance(dict_as_model, TestModel)
    assert hasattr(dict_as_model, "entity_id")
    assert hasattr(dict_as_model, "version")
    assert hasattr(dict_as_model, "previous_version")
    assert hasattr(dict_as_model, "changed_by_id")
    assert hasattr(dict_as_model, "changed_on")

    assert dict_as_model.entity_id == UUID(int=0)
    assert dict_as_model.version == UUID(int=0)
    assert dict_as_model.test_attribute == 5