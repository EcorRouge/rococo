"""
Test VersionedModel
"""
from datetime import datetime, timedelta
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

    model_as_dict = model.as_dict(True)

    assert isinstance(model_as_dict, dict)
    assert isinstance(model_as_dict['changed_on'], str)
