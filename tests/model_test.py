"""
Test VersionedModel
"""
from datetime import datetime, timedelta, timezone
from uuid import UUID
from dataclasses import dataclass, field, fields
import pytest
from unittest.mock import patch, MagicMock
from rococo.models import VersionedModel
from rococo.models.versioned_model import ModelValidationError, get_uuid_hex


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
    assert model.changed_on > datetime.now(
        timezone.utc) + timedelta(seconds=-1)


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
    model_dict = {"entity_id": UUID(int=0).hex, "version": UUID(int=0).hex}

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

    model_dict = {"entity_id": UUID(int=0).hex, "version": UUID(
        int=0).hex, "test_attribute": 5}
    dict_as_model = TestModel.from_dict(model_dict)

    assert isinstance(dict_as_model, TestModel)
    assert hasattr(dict_as_model, "entity_id")
    assert hasattr(dict_as_model, "version")
    assert hasattr(dict_as_model, "previous_version")
    assert hasattr(dict_as_model, "changed_by_id")
    assert hasattr(dict_as_model, "changed_on")

    assert dict_as_model.entity_id == UUID(int=0).hex
    assert dict_as_model.version == UUID(int=0).hex
    assert dict_as_model.test_attribute == 5


@patch('rococo.models.versioned_model.import_models_module')
@patch('importlib.import_module')
def test_post_init_model_resolution(mock_import_module, mock_import_models_module):
    """Test __post_init__ model class resolution from metadata"""

    @dataclass
    class TestModelWithRelation(VersionedModel):
        related_field: str = field(
            default="test",
            metadata={'relationship': {'model': 'SomeModel'}}
        )

    # Mock the modules
    mock_main_module = MagicMock()
    mock_models_module = MagicMock()
    mock_rococo_module = MagicMock()

    # Setup the model class
    mock_model_class = MagicMock()
    mock_rococo_module.SomeModel = mock_model_class

    # Need to provide enough side effects for all import_module calls
    mock_import_module.side_effect = [
        mock_main_module, mock_rococo_module, mock_rococo_module]
    mock_import_models_module.return_value = mock_models_module

    # Create instance
    model = TestModelWithRelation()

    # Verify the model class was resolved (it should be a mock object, not the string)
    field_metadata = next(f for f in fields(
        model) if f.name == 'related_field').metadata

    # The important thing is that it's no longer a string
    assert field_metadata['relationship']['model'] is not 'SomeModel'
    assert hasattr(field_metadata['relationship']['model'], '_mock_name')


def test_post_init_uuid_list_handling():
    """Test __post_init__ UUID list field handling"""
    from typing import List

    @dataclass
    class TestModelWithUUIDList(VersionedModel):
        uuid_list: List[UUID] = field(default=None)

    # Test with None value
    model1 = TestModelWithUUIDList()
    assert model1.uuid_list == []

    # Test with string representation of UUID list
    uuid_str = f"[{UUID(int=1)}, {UUID(int=2)}]"
    model2 = TestModelWithUUIDList(uuid_list=uuid_str)
    assert len(model2.uuid_list) == 2
    assert all(isinstance(u, UUID) for u in model2.uuid_list)


# Tests for item 4: VersionedModel.__getattribute__() Method
def test_getattribute_partial_instance_entity_id():
    """Test __getattribute__ allows entity_id access for partial instances"""
    model = VersionedModel(_is_partial=True)
    # Should not raise an exception
    entity_id = model.entity_id
    assert entity_id is not None


def test_getattribute_partial_instance_other_fields():
    """Test __getattribute__ restricts other field access for partial instances"""
    model = VersionedModel(_is_partial=True)

    with pytest.raises(AttributeError, match="Attribute 'version' is not available in a partial instance"):
        _ = model.version


def test_getattribute_m2m_field_unloaded():
    """Test __getattribute__ raises error for unloaded many-to-many fields"""

    @dataclass
    class TestModelWithM2M(VersionedModel):
        m2m_field: list = field(
            default=None,
            metadata={'field_type': 'm2m_list'}
        )

    model = TestModelWithM2M()

    with pytest.raises(AttributeError, match="Many-to-many field 'm2m_field' is not loaded"):
        _ = model.m2m_field


def test_getattribute_m2m_field_loaded():
    """Test __getattribute__ allows access to loaded many-to-many fields"""

    @dataclass
    class TestModelWithM2M(VersionedModel):
        m2m_field: list = field(
            default_factory=list,
            metadata={'field_type': 'm2m_list'}
        )

    model = TestModelWithM2M()
    # Should not raise an exception
    m2m_data = model.m2m_field
    assert m2m_data == []


# Tests for item 5: VersionedModel.__repr__() Method
def test_repr_partial_instance():
    """Test __repr__ for partial instances"""
    model = VersionedModel(_is_partial=True)
    repr_str = repr(model)
    expected = f"VersionedModel(entity_id='{model.entity_id}', _is_partial=True)"
    assert repr_str == expected


def test_repr_normal_instance():
    """Test __repr__ for normal instances"""
    model = VersionedModel()
    repr_str = repr(model)

    # Should contain all field names
    assert "VersionedModel(" in repr_str
    assert "entity_id=" in repr_str
    assert "version=" in repr_str
    assert "previous_version=" in repr_str
    assert "active=" in repr_str
    assert "changed_by_id=" in repr_str
    assert "changed_on=" in repr_str


def test_repr_m2m_field():
    """Test __repr__ shows [...] for many-to-many fields"""

    @dataclass
    class TestModelWithM2M(VersionedModel):
        m2m_field: list = field(
            default_factory=list,
            metadata={'field_type': 'm2m_list'}
        )

    model = TestModelWithM2M()
    repr_str = repr(model)
    # We never get [...], because get_attribute raises and Exception if m2m_field is None
    assert "m2m_field=[]" in repr_str


def test_repr_unloaded_field():
    """Test __repr__ behavior with unloaded many-to-many fields"""

    @dataclass
    class TestModelWithUnloaded(VersionedModel):
        unloaded_field: str = field(
            default=None,
            metadata={'field_type': 'm2m_list'}
        )

    model = TestModelWithUnloaded()

    # The __repr__ method will try to access the field and get an AttributeError
    # This tests that the __getattribute__ method properly raises the error
    with pytest.raises(AttributeError, match="Many-to-many field 'unloaded_field' is not loaded"):
        repr(model)


# Tests for item 6: VersionedModel.fields() Class Method
def test_fields_class_method():
    """Test fields() class method returns correct field names"""
    fields = VersionedModel.fields()
    expected_fields = ['entity_id', 'version',
                       'previous_version', 'active', 'changed_by_id', 'changed_on']
    assert fields == expected_fields


def test_fields_subclass():
    """Test fields() method works with subclasses"""

    @dataclass
    class TestSubclass(VersionedModel):
        custom_field: str = "test"

    fields = TestSubclass.fields()
    expected_fields = ['entity_id', 'version', 'previous_version',
                       'active', 'changed_by_id', 'changed_on', 'custom_field']
    assert fields == expected_fields


# Tests for item 7: VersionedModel.as_dict() Method - Advanced Features
def test_as_dict_partial_instance():
    """Test as_dict() for partial instances returns only entity_id"""
    model = VersionedModel(_is_partial=True)
    result = model.as_dict()
    assert result == {'entity_id': model.entity_id}


def test_as_dict_m2m_field_none():
    """Test as_dict() removes None many-to-many fields"""

    @dataclass
    class TestModelWithM2M(VersionedModel):
        m2m_field: list = field(
            default=None,
            metadata={'field_type': 'm2m_list'}
        )

    model = TestModelWithM2M()
    result = model.as_dict()
    assert 'm2m_field' not in result


def test_as_dict_m2m_field_with_objects():
    """Test as_dict() converts many-to-many field objects"""

    @dataclass
    class RelatedModel(VersionedModel):
        name: str = "related"

    @dataclass
    class TestModelWithM2M(VersionedModel):
        m2m_field: list = field(
            default_factory=list,
            metadata={'field_type': 'm2m_list'}
        )

    related = RelatedModel()
    model = TestModelWithM2M(m2m_field=[related])
    result = model.as_dict(convert_datetime_to_iso_string=True)

    assert 'm2m_field' in result
    assert isinstance(result['m2m_field'], list)
    assert len(result['m2m_field']) == 1


def test_as_dict_nested_versioned_model():
    """Test as_dict() handles nested VersionedModel instances"""

    @dataclass
    class NestedModel(VersionedModel):
        name: str = "nested"

    @dataclass
    class TestModelWithNested(VersionedModel):
        nested_field: NestedModel = field(
            default_factory=NestedModel,
            metadata={'field_type': 'record_id'}
        )

    model = TestModelWithNested()
    result = model.as_dict()

    assert 'nested_field' in result
    assert isinstance(result['nested_field'], dict)

    assert result["nested_field"]["name"] == "nested"


def test_as_dict_uuid_conversion():
    """Test as_dict() UUID conversion options"""
    model = VersionedModel()

    # Test with UUID conversion enabled (default)
    result_with_conversion = model.as_dict(convert_uuids=True)
    assert isinstance(result_with_conversion['entity_id'], str)


def test_as_dict_datetime_conversion():
    """Test as_dict() datetime to ISO string conversion"""
    model = VersionedModel()

    # Test with datetime conversion
    result = model.as_dict(convert_datetime_to_iso_string=True)
    assert isinstance(result['changed_on'], str)

    # Test without datetime conversion
    result = model.as_dict(convert_datetime_to_iso_string=False)
    assert isinstance(result['changed_on'], datetime)


# Tests for item 8: VersionedModel.from_dict() Method - Edge Cases
def test_from_dict_uuid_validation():
    """Test from_dict() UUID validation and error handling"""

    # Test with valid UUID string
    valid_data = {"entity_id": str(UUID(int=1))}
    model = VersionedModel.from_dict(valid_data)
    assert model.entity_id == UUID(int=1).hex

    # Test with invalid UUID string (should log error but not crash)
    invalid_data = {"entity_id": "invalid-uuid"}
    model = VersionedModel.from_dict(invalid_data)


def test_from_dict_field_filtering():
    """Test from_dict() filters out non-model fields"""

    @dataclass
    class TestModel(VersionedModel):
        valid_field: str = "test"

    data = {
        "valid_field": "value",
        "invalid_field": "should_be_filtered",
        "entity_id": UUID(int=1).hex
    }

    model = TestModel.from_dict(data)
    assert model.valid_field == "value"
    assert model.entity_id == UUID(int=1).hex
    assert not hasattr(model, 'invalid_field')


def test_from_dict_uuid_fields():
    """Test from_dict() handles all UUID fields correctly"""
    data = {
        "entity_id": str(UUID(int=1)),
        "version": str(UUID(int=2)),
        "previous_version": str(UUID(int=3)),
        "changed_by_id": str(UUID(int=4))
    }

    model = VersionedModel.from_dict(data)
    assert model.entity_id == UUID(int=1).hex
    assert model.version == UUID(int=2).hex
    assert model.previous_version == UUID(int=3).hex
    assert model.changed_by_id == UUID(int=4).hex


# Tests for item 9: VersionedModel.validate() Method
def test_validate_custom_field_validators():
    """Test validate() calls custom field validator methods"""

    @dataclass
    class TestModelWithValidator(VersionedModel):
        test_field: str = "test"

        def validate_test_field(self):
            if self.test_field == "invalid":
                return "Test field is invalid"
            return None

    # Test with valid value
    model = TestModelWithValidator(test_field="valid")
    model.validate()  # Should not raise

    # Test with invalid value
    model = TestModelWithValidator(test_field="invalid")
    with pytest.raises(ModelValidationError) as exc_info:
        model.validate()
    assert "Test field is invalid" in str(exc_info.value)


def test_validate_type_checking_disabled():
    """Test validate() with type checking disabled (default)"""

    @dataclass
    class TestModel(VersionedModel):
        int_field: int = 0

    # Type checking is disabled by default, so this should not raise
    model = TestModel(int_field="string_value")
    model.validate()  # Should not raise


def test_validate_type_checking_enabled():
    """Test validate() with type checking enabled"""

    @dataclass
    class TestModelWithTypeChecking(VersionedModel):
        use_type_checking = True
        int_field: int = 0
        str_field: str = "test"

    # Test with correct types - need to provide all required fields
    model = TestModelWithTypeChecking(
        int_field=42,
        str_field="hello",
        previous_version="test"  # Provide a string value for previous_version
    )
    model.validate()  # Should not raise

    # Test with wrong type that can be cast
    model = TestModelWithTypeChecking(
        int_field="42",
        str_field="hello",
        previous_version="test"  # Provide a string value for previous_version
    )
    model.validate()  # Should cast and not raise
    assert model.int_field == 42
    assert isinstance(model.int_field, int)


def test_validate_union_types():
    """Test validate() handles Union types correctly"""
    from typing import Union, Optional

    @dataclass
    class TestModelWithUnion(VersionedModel):
        use_type_checking = True
        optional_field: Optional[str] = None
        union_field: Union[int, str] = "test"

    # Test with None for optional field
    model = TestModelWithUnion(optional_field=None, previous_version="test")
    model.validate()  # Should not raise

    # Test with valid union types
    model = TestModelWithUnion(union_field=42, previous_version="test")
    model.validate()  # Should not raise

    model = TestModelWithUnion(union_field="string", previous_version="test")
    model.validate()  # Should not raise


def test_validate_enum_handling():
    """Test validate() handles Enum types correctly"""
    from enum import Enum

    class TestEnum(Enum):
        VALUE1 = "value1"
        VALUE2 = "value2"

    @dataclass
    class TestModelWithEnum(VersionedModel):
        use_type_checking = True
        enum_field: TestEnum = TestEnum.VALUE1

    # Test with enum value
    model = TestModelWithEnum(
        enum_field=TestEnum.VALUE2, previous_version="test")
    model.validate()  # Should not raise

    # Test with string that can be cast to enum
    model = TestModelWithEnum(enum_field="value2", previous_version="test")
    model.validate()  # Should cast and not raise
    assert model.enum_field == TestEnum.VALUE2


def test_validate_uuid_to_string_conversion():
    """Test validate() converts UUID to string when needed"""

    @dataclass
    class TestModelWithStringField(VersionedModel):
        use_type_checking = True
        string_field: str = "test"

    # Test UUID to string conversion
    uuid_val = UUID(int=1)
    model = TestModelWithStringField(
        string_field=uuid_val, previous_version="test")
    model.validate()
    # The validation converts UUID to string, but it uses str(uuid) not uuid.hex
    assert model.string_field == str(uuid_val)
    assert isinstance(model.string_field, str)


def test_validate_multiple_errors():
    """Test validate() collects multiple validation errors"""

    @dataclass
    class TestModelMultipleErrors(VersionedModel):
        use_type_checking = True
        field1: int = 0
        field2: str = "test"

        def validate_field1(self):
            return "Field1 error"

        def validate_field2(self):
            return "Field2 error"

    model = TestModelMultipleErrors()
    with pytest.raises(ModelValidationError) as exc_info:
        model.validate()

    error_str = str(exc_info.value)
    assert "Field1 error" in error_str
    assert "Field2 error" in error_str


def test_validate_none_values():
    """Test validate() handles None values correctly"""

    @dataclass
    class TestModelWithNone(VersionedModel):
        use_type_checking = True
        required_field: str = "test"

    model = TestModelWithNone(required_field=None)
    with pytest.raises(ModelValidationError) as exc_info:
        model.validate()
    assert "expected str, got NoneType" in str(exc_info.value)


# Tests for enum field conversion to and from dict
def test_enum_field_to_dict_conversion():
    """Test enum fields are properly converted to string values in as_dict()"""
    from enum import Enum
    from typing import Optional

    class OrganizationImportStatus(Enum):
        uploading = "uploading"
        canceled = "canceled"
        pending = "pending"

    @dataclass
    class OrganizationImport(VersionedModel):
        status: Optional[OrganizationImportStatus] = OrganizationImportStatus.pending

    # Test with enum value
    model = OrganizationImport(status=OrganizationImportStatus.uploading)
    result = model.as_dict()

    # The enum should be converted to its string value
    assert result['status'] == "uploading"
    assert isinstance(result['status'], str)

    # Test with None value
    model_none = OrganizationImport(status=None)
    result_none = model_none.as_dict()
    assert result_none['status'] is None


def test_enum_field_from_dict_conversion():
    """Test enum fields are properly converted from string values in from_dict()"""
    from enum import Enum
    from typing import Optional

    class OrganizationImportStatus(Enum):
        uploading = "uploading"
        canceled = "canceled"
        pending = "pending"

    @dataclass
    class OrganizationImport(VersionedModel):
        status: Optional[OrganizationImportStatus] = OrganizationImportStatus.pending

    # Test with string value that should become enum
    data = {
        "entity_id": "test-id",
        "status": "canceled"
    }

    model = OrganizationImport.from_dict(data)

    # The string should be converted to enum
    assert model.status == OrganizationImportStatus.canceled
    assert isinstance(model.status, OrganizationImportStatus)

    # Test with None value
    data_none = {
        "entity_id": "test-id",
        "status": None
    }

    model_none = OrganizationImport.from_dict(data_none)
    assert model_none.status is None


def test_enum_field_roundtrip_conversion():
    """Test enum fields maintain consistency through to_dict -> from_dict roundtrip"""
    from enum import Enum
    from typing import Optional

    class OrganizationImportStatus(Enum):
        uploading = "uploading"
        canceled = "canceled"
        pending = "pending"

    @dataclass
    class OrganizationImport(VersionedModel):
        status: Optional[OrganizationImportStatus] = OrganizationImportStatus.pending

    # Create original model
    original = OrganizationImport(status=OrganizationImportStatus.uploading)

    # Convert to dict and back
    dict_data = original.as_dict()
    restored = OrganizationImport.from_dict(dict_data)

    # Should maintain the same enum value
    assert restored.status == original.status
    assert restored.status == OrganizationImportStatus.uploading
    assert isinstance(restored.status, OrganizationImportStatus)


# Tests for dataclass field conversion to and from dict
def test_dataclass_field_to_dict_conversion():
    """Test dataclass fields are properly converted to dict in as_dict()"""
    from typing import Optional, List

    @dataclass
    class OrganizationImportError:
        message: Optional[str] = None
        code: Optional[int] = None

    @dataclass
    class OrganizationImport(VersionedModel):
        runtime_error: Optional[OrganizationImportError] = field(
            default=None, metadata={'model': OrganizationImportError}
        )
        errors: Optional[List[OrganizationImportError]] = field(
            default_factory=list, metadata={'model': OrganizationImportError}
        )

    # Test with single dataclass field
    error = OrganizationImportError(message="Test error", code=500)
    model = OrganizationImport(runtime_error=error)
    result = model.as_dict()

    # The dataclass should be converted to dict
    assert isinstance(result['runtime_error'], dict)
    assert result['runtime_error']['message'] == "Test error"
    assert result['runtime_error']['code'] == 500

    # Test with list of dataclass fields
    errors = [
        OrganizationImportError(message="Error 1", code=400),
        OrganizationImportError(message="Error 2", code=404)
    ]
    model_with_list = OrganizationImport(errors=errors)
    result_with_list = model_with_list.as_dict()

    # The list of dataclasses should be converted to list of dicts
    assert isinstance(result_with_list['errors'], list)
    assert len(result_with_list['errors']) == 2
    assert result_with_list['errors'][0]['message'] == "Error 1"
    assert result_with_list['errors'][0]['code'] == 400
    assert result_with_list['errors'][1]['message'] == "Error 2"
    assert result_with_list['errors'][1]['code'] == 404

    # Test with None values
    model_none = OrganizationImport(runtime_error=None, errors=None)
    result_none = model_none.as_dict()
    assert result_none['runtime_error'] is None
    assert result_none['errors'] is None


def test_dataclass_field_from_dict_conversion():
    """Test dataclass fields are properly converted from dict in from_dict()"""
    from typing import Optional, List

    @dataclass
    class OrganizationImportError:
        message: Optional[str] = None
        code: Optional[int] = None

    @dataclass
    class OrganizationImport(VersionedModel):
        runtime_error: Optional[OrganizationImportError] = field(
            default=None, metadata={'model': OrganizationImportError}
        )
        errors: Optional[List[OrganizationImportError]] = field(
            default_factory=list, metadata={'model': OrganizationImportError}
        )

    # Test with single dataclass field
    data = {
        "entity_id": "test-id",
        "runtime_error": {
            "message": "Test error",
            "code": 500
        }
    }

    model = OrganizationImport.from_dict(data)

    # The dict should be converted to dataclass
    assert isinstance(model.runtime_error, OrganizationImportError)
    assert model.runtime_error.message == "Test error"
    assert model.runtime_error.code == 500

    # Test with list of dataclass fields
    data_with_list = {
        "entity_id": "test-id",
        "errors": [
            {"message": "Error 1", "code": 400},
            {"message": "Error 2", "code": 404}
        ]
    }

    model_with_list = OrganizationImport.from_dict(data_with_list)

    # The list of dicts should be converted to list of dataclasses
    assert isinstance(model_with_list.errors, list)
    assert len(model_with_list.errors) == 2
    assert isinstance(model_with_list.errors[0], OrganizationImportError)
    assert model_with_list.errors[0].message == "Error 1"
    assert model_with_list.errors[0].code == 400
    assert isinstance(model_with_list.errors[1], OrganizationImportError)
    assert model_with_list.errors[1].message == "Error 2"
    assert model_with_list.errors[1].code == 404

    # Test with None values
    data_none = {
        "entity_id": "test-id",
        "runtime_error": None,
        "errors": None
    }

    model_none = OrganizationImport.from_dict(data_none)
    assert model_none.runtime_error is None
    assert model_none.errors is None


def test_dataclass_field_roundtrip_conversion():
    """Test dataclass fields maintain consistency through to_dict -> from_dict roundtrip"""
    from typing import Optional, List

    @dataclass
    class OrganizationImportError:
        message: Optional[str] = None
        code: Optional[int] = None

    @dataclass
    class OrganizationImport(VersionedModel):
        runtime_error: Optional[OrganizationImportError] = field(
            default=None, metadata={'model': OrganizationImportError}
        )
        errors: Optional[List[OrganizationImportError]] = field(
            default_factory=list, metadata={'model': OrganizationImportError}
        )

    # Create original model with both single and list dataclass fields
    error = OrganizationImportError(message="Runtime error", code=500)
    errors = [
        OrganizationImportError(message="Parse error 1", code=400),
        OrganizationImportError(message="Parse error 2", code=404)
    ]
    original = OrganizationImport(runtime_error=error, errors=errors)

    # Convert to dict and back
    dict_data = original.as_dict()
    restored = OrganizationImport.from_dict(dict_data)

    # Should maintain the same dataclass values
    assert isinstance(restored.runtime_error, OrganizationImportError)
    assert restored.runtime_error.message == original.runtime_error.message
    assert restored.runtime_error.code == original.runtime_error.code

    assert isinstance(restored.errors, list)
    assert len(restored.errors) == len(original.errors)
    for i, (restored_error, original_error) in enumerate(zip(restored.errors, original.errors)):
        assert isinstance(restored_error, OrganizationImportError)
        assert restored_error.message == original_error.message
        assert restored_error.code == original_error.code


def test_dataclass_field_without_metadata():
    """Test that dataclass fields without 'model' metadata are not converted"""
    from typing import Optional

    @dataclass
    class OrganizationImportError:
        message: Optional[str] = None

    @dataclass
    class OrganizationImport(VersionedModel):
        # No metadata specified - should not be converted
        runtime_error: Optional[OrganizationImportError] = None

    # Test as_dict - should remain as dataclass object
    error = OrganizationImportError(message="Test error")
    model = OrganizationImport(runtime_error=error)
    result = model.as_dict()

    # Without metadata, the dataclass should remain as-is
    assert isinstance(result['runtime_error'], OrganizationImportError)
    assert result['runtime_error'].message == "Test error"

    # Test from_dict - should remain as dict
    data = {
        "entity_id": "test-id",
        "runtime_error": {"message": "Test error"}
    }

    restored = OrganizationImport.from_dict(data)
    # Without metadata, the dict should remain as dict
    assert isinstance(restored.runtime_error, dict)
    assert restored.runtime_error['message'] == "Test error"


# Tests for extra fields support
def test_extra_fields_allowed():
    """Test that models with allow_extra=True accept extra fields"""

    @dataclass
    class ModelWithExtra(VersionedModel):
        allow_extra = True
        name: str = "test"

    # Test from_dict with extra fields
    data = {
        "entity_id": "test-id",
        "name": "test_model",
        "extra_field1": "extra_value1",
        "extra_field2": 42,
        "extra_field3": {"nested": "data"}
    }

    model = ModelWithExtra.from_dict(data)

    # Regular fields should be set normally
    assert model.name == "test_model"

    # Extra fields should be stored in the extra dict (inherited from VersionedModel)
    assert "extra_field1" in model.extra
    assert model.extra["extra_field1"] == "extra_value1"
    assert model.extra["extra_field2"] == 42
    assert model.extra["extra_field3"] == {"nested": "data"}


def test_extra_fields_in_as_dict():
    """Test that extra fields are unwrapped in as_dict()"""

    @dataclass
    class ModelWithExtra(VersionedModel):
        allow_extra = True
        name: str = "test"

    # Create model with extra fields
    model = ModelWithExtra(name="test_model")
    model.extra = {
        "extra_field1": "extra_value1",
        "extra_field2": 42,
        "extra_field3": {"nested": "data"}
    }

    result = model.as_dict()

    print(f"Result={result}")

    # Regular fields should be present
    assert result["name"] == "test_model"

    # Extra fields should be unwrapped (not nested under 'extra')
    assert result["extra_field1"] == "extra_value1"
    assert result["extra_field2"] == 42
    assert result["extra_field3"] == {"nested": "data"}

    # The 'extra' field itself should not be in the result
    assert "extra" not in result


def test_extra_fields_roundtrip():
    """Test that extra fields maintain consistency through roundtrip"""

    @dataclass
    class ModelWithExtra(VersionedModel):
        allow_extra = True
        name: str = "test"

    # Create original model with extra fields
    original = ModelWithExtra(name="original_model")
    original.extra = {
        "dynamic_field": "dynamic_value",
        "computed_score": 95.5,
        "metadata": {"source": "api", "version": "1.0"}
    }

    # Convert to dict and back
    dict_data = original.as_dict()
    restored = ModelWithExtra.from_dict(dict_data)

    # Verify all data is correctly restored
    assert restored.name == original.name
    assert restored.extra == original.extra
    assert restored.extra["dynamic_field"] == "dynamic_value"
    assert restored.extra["computed_score"] == 95.5
    assert restored.extra["metadata"]["source"] == "api"


def test_extra_fields_not_allowed():
    """Test that models without allow_extra ignore extra fields"""

    @dataclass
    class ModelWithoutExtra(VersionedModel):
        # allow_extra is False by default
        name: str = "test"

    # Test from_dict with extra fields - should be ignored
    data = {
        "entity_id": "test-id",
        "name": "test_model",
        "extra_field1": "should_be_ignored",
        "extra_field2": 42
    }

    model = ModelWithoutExtra.from_dict(data)

    # Regular fields should be set normally
    assert model.name == "test_model"

    # Extra fields should be ignored (not cause errors)
    assert not hasattr(model, 'extra_field1')
    assert not hasattr(model, 'extra_field2')

    # The extra dict should remain empty
    assert model.extra == {}


def test_extra_fields_with_explicit_extra_field():
    """Test that models can have explicit 'extra' field data in from_dict"""

    @dataclass
    class ModelWithExplicitExtra(VersionedModel):
        allow_extra = True
        name: str = "test"

    # Test from_dict with both explicit extra and additional fields
    data = {
        "entity_id": "test-id",
        "name": "test_model",
        "extra": {"explicit": "extra_data"},
        "additional_field": "additional_value"
    }

    restored = ModelWithExplicitExtra.from_dict(data)

    # Both explicit extra and additional fields should be in extra
    assert "explicit" in restored.extra
    assert restored.extra["explicit"] == "extra_data"
    assert "additional_field" in restored.extra
    assert restored.extra["additional_field"] == "additional_value"


def test_extra_fields_empty():
    """Test that empty extra fields work correctly"""

    @dataclass
    class ModelWithExtra(VersionedModel):
        allow_extra = True
        name: str = "test"

    # Test with no extra fields
    data = {
        "entity_id": "test-id",
        "name": "test_model"
    }

    model = ModelWithExtra.from_dict(data)
    assert model.extra == {}

    # Test as_dict with empty extra
    result = model.as_dict()
    assert "extra" not in result
    assert result["name"] == "test_model"


def test_extra_fields_with_enum_and_dataclass():
    """Test that extra fields work alongside enum and dataclass conversion"""
    from typing import Optional
    from enum import Enum

    class Status(Enum):
        active = "active"
        inactive = "inactive"

    @dataclass
    class ErrorInfo:
        message: str = "error"

    @dataclass
    class ComplexModelWithExtra(VersionedModel):
        allow_extra = True
        status: Optional[Status] = Status.active
        error: Optional[ErrorInfo] = field(
            default=None, metadata={'model': ErrorInfo}
        )

    # Test from_dict with mixed field types
    data = {
        "entity_id": "test-id",
        "status": "inactive",  # Should become enum
        "error": {"message": "test error"},  # Should become dataclass
        "custom_field": "custom_value",  # Should go to extra
        "dynamic_config": {"setting": "value"}  # Should go to extra
    }

    model = ComplexModelWithExtra.from_dict(data)

    # Verify enum conversion
    assert model.status == Status.inactive
    assert isinstance(model.status, Status)

    # Verify dataclass conversion
    assert isinstance(model.error, ErrorInfo)
    assert model.error.message == "test error"

    # Verify extra fields
    assert model.extra["custom_field"] == "custom_value"
    assert model.extra["dynamic_config"] == {"setting": "value"}

    # Test as_dict roundtrip
    result = model.as_dict()

    # Enum should be string
    assert result["status"] == "inactive"

    # Dataclass should be dict
    assert result["error"] == {"message": "test error"}

    # Extra fields should be unwrapped
    assert result["custom_field"] == "custom_value"
    assert result["dynamic_config"] == {"setting": "value"}
    assert "extra" not in result


def test_extra_fields_base_class_inheritance():
    """Test that extra field is properly inherited from VersionedModel"""

    @dataclass
    class SimpleModel(VersionedModel):
        allow_extra = True
        name: str = "test"

    # Verify that extra field exists and is properly typed
    model = SimpleModel(name="test")
    assert hasattr(model, 'extra')
    assert isinstance(model.extra, dict)
    assert model.extra == {}

    # Verify it works with assignment
    model.extra["test_key"] = "test_value"
    assert model.extra["test_key"] == "test_value"


# Tests for direct attribute access to extra fields
def test_extra_fields_direct_attribute_access():
    """Test that extra fields can be accessed directly as attributes"""

    @dataclass
    class ModelWithExtra(VersionedModel):
        allow_extra = True
        name: str = "test"

    # Load model with extra fields from dict
    data = {
        "name": "test_model",
        "custom_field": "custom_value",
        "dynamic_config": {"setting": "value"},
        "score": 95.5
    }

    model = ModelWithExtra.from_dict(data)

    # Test direct attribute access to extra fields
    assert model.custom_field == "custom_value"
    assert model.dynamic_config == {"setting": "value"}
    assert model.score == 95.5

    # Test that regular fields still work
    assert model.name == "test_model"


def test_extra_fields_direct_attribute_setting():
    """Test that extra fields can be set directly as attributes"""

    @dataclass
    class ModelWithExtra(VersionedModel):
        allow_extra = True
        name: str = "test"

    model = ModelWithExtra(name="test_model")

    # Set extra fields directly as attributes
    model.new_field = "new_value"
    model.rating = 4.5
    model.config = {"enabled": True}

    # Verify they are stored in the extra dict
    assert model.extra["new_field"] == "new_value"
    assert model.extra["rating"] == 4.5
    assert model.extra["config"] == {"enabled": True}

    # Verify direct access works
    assert model.new_field == "new_value"
    assert model.rating == 4.5
    assert model.config == {"enabled": True}


def test_extra_fields_direct_access_without_allow_extra():
    """Test that models without allow_extra don't allow direct extra field access"""

    @dataclass
    class ModelWithoutExtra(VersionedModel):
        name: str = "test"

    data = {
        "name": "test_model",
        "custom_field": "custom_value"
    }

    model = ModelWithoutExtra.from_dict(data)

    # Extra fields should not be accessible directly
    with pytest.raises(AttributeError):
        _ = model.custom_field

    # Setting extra fields should work but they go to regular attributes
    model.test_field = "test_value"
    assert model.test_field == "test_value"
    # They should not be in the extra dict
    assert model.extra == {}


def test_extra_fields_direct_access_roundtrip():
    """Test that direct attribute access works through roundtrip conversion"""

    @dataclass
    class ModelWithExtra(VersionedModel):
        allow_extra = True
        name: str = "test"

    # Create model and set extra fields directly
    original = ModelWithExtra(name="original_model")
    original.dynamic_field = "dynamic_value"
    original.score = 95.5

    # Convert to dict and back
    dict_data = original.as_dict()
    restored = ModelWithExtra.from_dict(dict_data)

    # Verify direct access works on restored model
    assert restored.dynamic_field == "dynamic_value"
    assert restored.score == 95.5
    assert restored.name == "original_model"


def test_extra_fields_direct_access_attribute_error():
    """Test that accessing non-existent extra fields raises AttributeError"""

    @dataclass
    class ModelWithExtra(VersionedModel):
        allow_extra = True
        name: str = "test"

    model = ModelWithExtra(name="test_model")

    # Accessing non-existent field should raise AttributeError
    with pytest.raises(AttributeError, match="'ModelWithExtra' object has no attribute 'non_existent'"):
        _ = model.non_existent


def test_extra_fields_direct_access_with_regular_fields():
    """Test that direct access doesn't interfere with regular model fields"""

    @dataclass
    class ModelWithExtra(VersionedModel):
        allow_extra = True
        name: str = "test"
        regular_field: str = "regular"

    model = ModelWithExtra(name="test_model", regular_field="regular_value")

    # Set extra field with same name as regular field - should not interfere
    model.extra_field = "extra_value"

    # Regular fields should work normally
    assert model.name == "test_model"
    assert model.regular_field == "regular_value"

    # Extra field should work via direct access
    assert model.extra_field == "extra_value"

    # Extra field should be in extra dict
    assert model.extra["extra_field"] == "extra_value"

    # Regular fields should not be in extra dict
    assert "name" not in model.extra
    assert "regular_field" not in model.extra


# Tests for export_properties parameter in as_dict()
def test_as_dict_export_properties_default():
    """Test that as_dict() includes properties by default (export_properties=True)"""

    @dataclass
    class ModelWithProperties(VersionedModel):
        name: str = "test"

        @property
        def full_name(self):
            return f"Full: {self.name}"

        @property
        def calculated_score(self):
            return len(self.name) * 10

    model = ModelWithProperties(name="TestName")
    result = model.as_dict()

    # Properties should be included by default
    assert "full_name" in result
    assert result["full_name"] == "Full: TestName"
    assert "calculated_score" in result
    assert result["calculated_score"] == 80  # len("TestName") * 10

    # Regular fields should also be present
    assert "name" in result
    assert result["name"] == "TestName"


def test_as_dict_export_properties_true():
    """Test that as_dict(export_properties=True) includes properties"""

    @dataclass
    class ModelWithProperties(VersionedModel):
        name: str = "test"

        @property
        def computed_value(self):
            return f"computed_{self.name}"

    model = ModelWithProperties(name="TestValue")
    result = model.as_dict(export_properties=True)

    # Properties should be included when explicitly set to True
    assert "computed_value" in result
    assert result["computed_value"] == "computed_TestValue"
    assert "name" in result
    assert result["name"] == "TestValue"


def test_as_dict_export_properties_false():
    """Test that as_dict(export_properties=False) excludes properties"""

    @dataclass
    class ModelWithProperties(VersionedModel):
        name: str = "test"

        @property
        def computed_value(self):
            return f"computed_{self.name}"

        @property
        def another_property(self):
            return "another_value"

    model = ModelWithProperties(name="TestValue")
    result = model.as_dict(export_properties=False)

    # Properties should be excluded when set to False
    assert "computed_value" not in result
    assert "another_property" not in result

    # Regular fields should still be present
    assert "name" in result
    assert result["name"] == "TestValue"

    # Standard VersionedModel fields should be present
    assert "entity_id" in result
    assert "version" in result


def test_as_dict_export_properties_with_complex_properties():
    """Test export_properties with properties that return complex objects"""

    @dataclass
    class ModelWithComplexProperties(VersionedModel):
        items: list = field(default_factory=list)

        @property
        def item_count(self):
            return len(self.items)

        @property
        def item_summary(self):
            return {
                "count": len(self.items),
                "first_item": self.items[0] if self.items else None
            }

    model = ModelWithComplexProperties(items=["item1", "item2", "item3"])

    # Test with properties included
    result_with_props = model.as_dict(export_properties=True)
    assert "item_count" in result_with_props
    assert result_with_props["item_count"] == 3
    assert "item_summary" in result_with_props
    assert result_with_props["item_summary"]["count"] == 3
    assert result_with_props["item_summary"]["first_item"] == "item1"

    # Test with properties excluded
    result_without_props = model.as_dict(export_properties=False)
    assert "item_count" not in result_without_props
    assert "item_summary" not in result_without_props
    assert "items" in result_without_props
    assert result_without_props["items"] == ["item1", "item2", "item3"]


def test_as_dict_export_properties_with_property_exceptions():
    """Test that properties that raise exceptions are handled gracefully"""

    @dataclass
    class ModelWithExceptionProperty(VersionedModel):
        name: str = "test"

        @property
        def good_property(self):
            return f"good_{self.name}"

        @property
        def bad_property(self):
            raise ValueError("This property always fails")

    model = ModelWithExceptionProperty(name="TestValue")

    # With properties enabled, should handle exceptions gracefully
    result = model.as_dict(export_properties=True)

    # Good property should be included
    assert "good_property" in result
    assert result["good_property"] == "good_TestValue"

    # Bad property should be excluded (exception handled)
    assert "bad_property" not in result

    # Regular fields should still work
    assert "name" in result
    assert result["name"] == "TestValue"


def test_as_dict_export_properties_inheritance():
    """Test export_properties works with inherited properties"""

    @dataclass
    class BaseModelWithProperty(VersionedModel):
        base_field: str = "base"

        @property
        def base_property(self):
            return f"base_{self.base_field}"

    @dataclass
    class DerivedModelWithProperty(BaseModelWithProperty):
        derived_field: str = "derived"

        @property
        def derived_property(self):
            return f"derived_{self.derived_field}"

    model = DerivedModelWithProperty(
        base_field="base_value", derived_field="derived_value")

    # Test with properties included
    result_with_props = model.as_dict(export_properties=True)
    assert "base_property" in result_with_props
    assert result_with_props["base_property"] == "base_base_value"
    assert "derived_property" in result_with_props
    assert result_with_props["derived_property"] == "derived_derived_value"

    # Test with properties excluded
    result_without_props = model.as_dict(export_properties=False)
    assert "base_property" not in result_without_props
    assert "derived_property" not in result_without_props
    assert "base_field" in result_without_props
    assert "derived_field" in result_without_props


# Tests for repository integration with save_calculated_fields
def test_repository_save_calculated_fields_default():
    """Test that BaseRepository.save_calculated_fields defaults to False"""
    from rococo.repositories.base_repository import BaseRepository
    from rococo.data.base import DbAdapter
    from rococo.messaging.base import MessageAdapter
    from unittest.mock import MagicMock

    # Create mock adapters
    mock_db_adapter = MagicMock(spec=DbAdapter)
    mock_message_adapter = MagicMock(spec=MessageAdapter)

    @dataclass
    class TestModel(VersionedModel):
        name: str = "test"

    # Create repository
    repo = BaseRepository(
        adapter=mock_db_adapter,
        model=TestModel,
        message_adapter=mock_message_adapter,
        queue_name="test_queue"
    )

    # Verify default value
    assert repo.save_calculated_fields == False


def test_repository_process_data_before_save_excludes_properties_by_default():
    """Test that BaseRepository._process_data_before_save excludes properties by default"""
    from rococo.repositories.base_repository import BaseRepository
    from rococo.data.base import DbAdapter
    from rococo.messaging.base import MessageAdapter
    from unittest.mock import MagicMock
    from uuid import uuid4

    # Create mock adapters
    mock_db_adapter = MagicMock(spec=DbAdapter)
    mock_message_adapter = MagicMock(spec=MessageAdapter)

    @dataclass
    class TestModelWithProperties(VersionedModel):
        name: str = "test"

        @property
        def computed_field(self):
            return f"computed_{self.name}"

        @property
        def calculated_score(self):
            return len(self.name) * 10

    # Create repository
    repo = BaseRepository(
        adapter=mock_db_adapter,
        model=TestModelWithProperties,
        message_adapter=mock_message_adapter,
        queue_name="test_queue",
        user_id=uuid4()
    )

    # Create test model
    model = TestModelWithProperties(name="TestName")

    # Process data before save
    processed_data = repo._process_data_before_save(model)

    # Properties should be excluded by default (save_calculated_fields=False)
    assert "computed_field" not in processed_data
    assert "calculated_score" not in processed_data

    # Regular fields should be present
    assert "name" in processed_data
    assert processed_data["name"] == "TestName"

    # Standard VersionedModel fields should be present
    assert "entity_id" in processed_data
    assert "version" in processed_data


def test_repository_process_data_before_save_includes_properties_when_enabled():
    """Test that BaseRepository._process_data_before_save includes properties when save_calculated_fields=True"""
    from rococo.repositories.base_repository import BaseRepository
    from rococo.data.base import DbAdapter
    from rococo.messaging.base import MessageAdapter
    from unittest.mock import MagicMock
    from uuid import uuid4

    # Create mock adapters
    mock_db_adapter = MagicMock(spec=DbAdapter)
    mock_message_adapter = MagicMock(spec=MessageAdapter)

    @dataclass
    class TestModelWithProperties(VersionedModel):
        name: str = "test"

        @property
        def computed_field(self):
            return f"computed_{self.name}"

        @property
        def calculated_score(self):
            return len(self.name) * 10

    # Create repository
    repo = BaseRepository(
        adapter=mock_db_adapter,
        model=TestModelWithProperties,
        message_adapter=mock_message_adapter,
        queue_name="test_queue",
        user_id=uuid4()
    )

    # Enable saving calculated fields
    repo.save_calculated_fields = True

    # Create test model
    model = TestModelWithProperties(name="TestName")

    # Process data before save
    processed_data = repo._process_data_before_save(model)

    # Properties should be included when save_calculated_fields=True
    assert "computed_field" in processed_data
    assert processed_data["computed_field"] == "computed_TestName"
    assert "calculated_score" in processed_data
    assert processed_data["calculated_score"] == 80  # len("TestName") * 10

    # Regular fields should still be present
    assert "name" in processed_data
    assert processed_data["name"] == "TestName"


def test_repository_save_calculated_fields_can_be_set():
    """Test that BaseRepository.save_calculated_fields can be modified"""
    from rococo.repositories.base_repository import BaseRepository
    from rococo.data.base import DbAdapter
    from rococo.messaging.base import MessageAdapter
    from unittest.mock import MagicMock

    # Create mock adapters
    mock_db_adapter = MagicMock(spec=DbAdapter)
    mock_message_adapter = MagicMock(spec=MessageAdapter)

    @dataclass
    class TestModel(VersionedModel):
        name: str = "test"

    # Create repository
    repo = BaseRepository(
        adapter=mock_db_adapter,
        model=TestModel,
        message_adapter=mock_message_adapter,
        queue_name="test_queue"
    )

    # Verify default value
    assert repo.save_calculated_fields == False

    # Change the value
    repo.save_calculated_fields = True
    assert repo.save_calculated_fields == True

    # Change back
    repo.save_calculated_fields = False
    assert repo.save_calculated_fields == False


def test_repository_integration_with_enum_dataclass_and_properties():
    """Test repository integration with all enhanced features: enum, dataclass, properties, and extra fields"""
    from rococo.repositories.base_repository import BaseRepository
    from rococo.data.base import DbAdapter
    from rococo.messaging.base import MessageAdapter
    from unittest.mock import MagicMock
    from uuid import uuid4
    from enum import Enum
    from typing import Optional, List

    class Status(Enum):
        active = "active"
        inactive = "inactive"

    @dataclass
    class ErrorInfo:
        message: str = "error"
        code: int = 500

    @dataclass
    class ComplexModelWithAllFeatures(VersionedModel):
        allow_extra = True
        status: Optional[Status] = Status.active
        error: Optional[ErrorInfo] = field(
            default=None, metadata={'model': ErrorInfo}
        )
        name: str = "test"

        @property
        def status_display(self):
            return f"Status: {self.status.value if self.status else 'None'}"

        @property
        def error_summary(self):
            if self.error:
                return f"{self.error.message} ({self.error.code})"
            return "No errors"

    # Create mock adapters
    mock_db_adapter = MagicMock(spec=DbAdapter)
    mock_message_adapter = MagicMock(spec=MessageAdapter)

    # Create repository with properties disabled (default)
    repo = BaseRepository(
        adapter=mock_db_adapter,
        model=ComplexModelWithAllFeatures,
        message_adapter=mock_message_adapter,
        queue_name="test_queue",
        user_id=uuid4()
    )

    # Create complex model
    model = ComplexModelWithAllFeatures(
        status=Status.inactive,
        error=ErrorInfo(message="Test error", code=404),
        name="complex_test"
    )
    model.extra_field = "extra_value"

    # Test with properties disabled (default repository behavior)
    processed_data_no_props = repo._process_data_before_save(model)

    # Enum should be converted to string
    assert processed_data_no_props["status"] == "inactive"

    # Dataclass should be converted to dict
    assert isinstance(processed_data_no_props["error"], dict)
    assert processed_data_no_props["error"]["message"] == "Test error"
    assert processed_data_no_props["error"]["code"] == 404

    # Extra fields should be unwrapped
    assert processed_data_no_props["extra_field"] == "extra_value"

    # Properties should be excluded
    assert "status_display" not in processed_data_no_props
    assert "error_summary" not in processed_data_no_props

    # Test with properties enabled
    repo.save_calculated_fields = True
    processed_data_with_props = repo._process_data_before_save(model)

    # All previous assertions should still hold
    assert processed_data_with_props["status"] == "inactive"
    assert processed_data_with_props["error"]["message"] == "Test error"
    assert processed_data_with_props["extra_field"] == "extra_value"

    # Properties should now be included
    assert "status_display" in processed_data_with_props
    assert processed_data_with_props["status_display"] == "Status: inactive"
    assert "error_summary" in processed_data_with_props
    assert processed_data_with_props["error_summary"] == "Test error (404)"


def test_as_dict_partial_instance_with_properties():
    """Test that partial instances handle properties correctly"""

    @dataclass
    class ModelWithProperties(VersionedModel):
        name: str = "test"

        @property
        def computed_name(self):
            return f"computed_{self.name}"

    # Create partial instance
    model = ModelWithProperties(_is_partial=True)

    # as_dict on partial instance should only return entity_id
    result = model.as_dict()
    assert result == {'entity_id': model.entity_id}

    # Properties should not be included even if export_properties=True
    result_with_props = model.as_dict(export_properties=True)
    assert result_with_props == {'entity_id': model.entity_id}
    assert "computed_name" not in result_with_props
    assert "name" not in result_with_props


# Tests for field alias functionality
def test_field_alias_in_as_dict():
    """Test that field aliases are used in as_dict() output"""

    @dataclass
    class ModelWithAliases(VersionedModel):
        name: str = field(default="test", metadata={'alias': 'display_name'})
        user_email: str = field(
            default="test@example.com", metadata={'alias': 'email'})
        score: int = field(default=100, metadata={'alias': 'rating'})
        # Field without alias
        description: str = "test description"

    model = ModelWithAliases(
        name="Test User",
        user_email="user@test.com",
        score=95,
        description="A test user"
    )

    result = model.as_dict()

    # Aliased fields should use their aliases as keys
    assert "display_name" in result
    assert result["display_name"] == "Test User"
    assert "email" in result
    assert result["email"] == "user@test.com"
    assert "rating" in result
    assert result["rating"] == 95

    # Original field names should not be present for aliased fields
    assert "name" not in result
    assert "user_email" not in result
    assert "score" not in result

    # Non-aliased fields should use original names
    assert "description" in result
    assert result["description"] == "A test user"

    # Big 6 fields should always use original names (no aliases)
    assert "entity_id" in result
    assert "version" in result
    assert "active" in result
    assert "changed_on" in result
    assert "changed_by_id" in result


def test_field_alias_in_from_dict():
    """Test that field aliases are handled correctly in from_dict()"""

    @dataclass
    class ModelWithAliases(VersionedModel):
        name: str = field(default="test", metadata={'alias': 'display_name'})
        user_email: str = field(
            default="test@example.com", metadata={'alias': 'email'})
        score: int = field(default=100, metadata={'alias': 'rating'})
        description: str = "test description"

    # Create data using aliases
    data = {
        "entity_id": "test-id",
        "display_name": "Test User",
        "email": "user@test.com",
        "rating": 95,
        "description": "A test user",
        # Big 6 fields should use original names
        "active": True,
        "version": "test-version"
    }

    model = ModelWithAliases.from_dict(data)

    # Fields should be correctly mapped from aliases to original field names
    assert model.name == "Test User"
    assert model.user_email == "user@test.com"
    assert model.score == 95
    assert model.description == "A test user"

    # Big 6 fields should work normally
    assert model.entity_id == "test-id"
    assert model.active == True
    assert model.version == "test-version"


def test_field_alias_roundtrip_conversion():
    """Test that field aliases work correctly through as_dict -> from_dict roundtrip"""

    @dataclass
    class ModelWithAliases(VersionedModel):
        full_name: str = field(default="test", metadata={'alias': 'name'})
        contact_email: str = field(
            default="test@example.com", metadata={'alias': 'email'})
        user_score: int = field(default=100, metadata={'alias': 'score'})
        notes: str = "test notes"

    # Create original model
    original = ModelWithAliases(
        full_name="John Doe",
        contact_email="john@example.com",
        user_score=85,
        notes="Test user notes"
    )

    # Convert to dict (should use aliases)
    dict_data = original.as_dict()

    # Verify aliases are used in dict
    assert "name" in dict_data
    assert dict_data["name"] == "John Doe"
    assert "email" in dict_data
    assert dict_data["email"] == "john@example.com"
    assert "score" in dict_data
    assert dict_data["score"] == 85
    assert "notes" in dict_data
    assert dict_data["notes"] == "Test user notes"

    # Original field names should not be in dict
    assert "full_name" not in dict_data
    assert "contact_email" not in dict_data
    assert "user_score" not in dict_data

    # Convert back from dict (should handle aliases)
    restored = ModelWithAliases.from_dict(dict_data)

    # Verify all data is correctly restored
    assert restored.full_name == original.full_name
    assert restored.contact_email == original.contact_email
    assert restored.user_score == original.user_score
    assert restored.notes == original.notes
    assert restored.entity_id == original.entity_id
    assert restored.active == original.active


def test_field_alias_with_mixed_data():
    """Test field aliases work when data contains both aliased and original field names"""

    @dataclass
    class ModelWithAliases(VersionedModel):
        user_name: str = field(default="test", metadata={'alias': 'name'})
        user_age: int = field(default=25, metadata={'alias': 'age'})

    # Data with mixed aliased and original field names
    data = {
        "entity_id": "test-id",
        "name": "Alice",  # Using alias
        "user_age": 30,   # Using original field name
        "active": True
    }

    model = ModelWithAliases.from_dict(data)

    # Alias should take precedence, but original field name should also work
    assert model.user_name == "Alice"  # From alias 'name'
    assert model.user_age == 30        # From original field name
    assert model.entity_id == "test-id"
    assert model.active == True


def test_field_alias_big_6_fields_not_aliased():
    """Test that Big 6 fields (entity_id, version, etc.) are never aliased"""

    @dataclass
    class ModelWithAliases(VersionedModel):
        # Try to add aliases to Big 6 fields (should be ignored)
        entity_id: str = field(
            default_factory=get_uuid_hex, metadata={'alias': 'id'})
        version: str = field(default_factory=lambda: get_uuid_hex(
            0), metadata={'alias': 'ver'})
        active: bool = field(default=True, metadata={'alias': 'is_active'})
        # Custom field with alias
        name: str = field(default="test", metadata={'alias': 'display_name'})

    model = ModelWithAliases(name="Test User")
    result = model.as_dict()

    # Big 6 fields should always use original names, never aliases
    assert "entity_id" in result
    assert "version" in result
    assert "active" in result
    assert "id" not in result
    assert "ver" not in result
    assert "is_active" not in result

    # Custom fields should use aliases
    assert "display_name" in result
    assert result["display_name"] == "Test User"
    assert "name" not in result


def test_field_alias_with_none_values():
    """Test field aliases work correctly with None values"""

    @dataclass
    class ModelWithAliases(VersionedModel):
        optional_name: str = field(default=None, metadata={'alias': 'name'})
        optional_score: int = field(default=None, metadata={'alias': 'rating'})

    model = ModelWithAliases(optional_name=None, optional_score=None)
    result = model.as_dict()

    # Aliases should be used even for None values
    assert "name" in result
    assert result["name"] is None
    assert "rating" in result
    assert result["rating"] is None

    # Original field names should not be present
    assert "optional_name" not in result
    assert "optional_score" not in result


def test_field_alias_with_complex_types():
    """Test field aliases work with complex field types (enums, dataclasses, etc.)"""
    from enum import Enum
    from typing import Optional

    class Status(Enum):
        active = "active"
        inactive = "inactive"

    @dataclass
    class ContactInfo:
        phone: str = "555-0123"
        address: str = "123 Main St"

    @dataclass
    class ModelWithComplexAliases(VersionedModel):
        user_status: Optional[Status] = field(
            default=Status.active, metadata={'alias': 'status'})
        contact_details: Optional[ContactInfo] = field(
            default=None,
            metadata={'alias': 'contact', 'model': ContactInfo}
        )
        tags: list = field(default_factory=list, metadata={'alias': 'labels'})

    contact = ContactInfo(phone="555-9999", address="456 Oak Ave")
    model = ModelWithComplexAliases(
        user_status=Status.inactive,
        contact_details=contact,
        tags=["important", "customer"]
    )

    result = model.as_dict()

    # Enum field should use alias and be converted to string
    assert "status" in result
    assert result["status"] == "inactive"
    assert "user_status" not in result

    # Dataclass field should use alias and be converted to dict
    assert "contact" in result
    assert isinstance(result["contact"], dict)
    assert result["contact"]["phone"] == "555-9999"
    assert result["contact"]["address"] == "456 Oak Ave"
    assert "contact_details" not in result

    # List field should use alias
    assert "labels" in result
    assert result["labels"] == ["important", "customer"]
    assert "tags" not in result

    # Test roundtrip
    restored = ModelWithComplexAliases.from_dict(result)
    assert restored.user_status == Status.inactive
    assert isinstance(restored.contact_details, ContactInfo)
    assert restored.contact_details.phone == "555-9999"
    assert restored.tags == ["important", "customer"]


def test_field_alias_with_extra_fields():
    """Test field aliases work correctly with extra fields"""

    @dataclass
    class ModelWithAliasesAndExtra(VersionedModel):
        allow_extra = True
        name: str = field(default="test", metadata={'alias': 'display_name'})
        score: int = field(default=100, metadata={'alias': 'rating'})

    # Create model with extra fields
    model = ModelWithAliasesAndExtra(name="Test User", score=95)
    model.extra = {
        "custom_field": "custom_value",
        "dynamic_data": {"nested": "value"}
    }

    result = model.as_dict()

    # Aliased fields should use aliases
    assert "display_name" in result
    assert result["display_name"] == "Test User"
    assert "rating" in result
    assert result["rating"] == 95

    # Extra fields should be unwrapped normally
    assert "custom_field" in result
    assert result["custom_field"] == "custom_value"
    assert "dynamic_data" in result
    assert result["dynamic_data"] == {"nested": "value"}

    # Test roundtrip with extra fields
    restored = ModelWithAliasesAndExtra.from_dict(result)
    assert restored.name == "Test User"
    assert restored.score == 95
    assert restored.extra["custom_field"] == "custom_value"
    assert restored.extra["dynamic_data"] == {"nested": "value"}


def test_field_alias_with_properties():
    """Test field aliases work correctly with @property methods"""

    @dataclass
    class ModelWithAliasesAndProperties(VersionedModel):
        first_name: str = field(default="John", metadata={'alias': 'fname'})
        last_name: str = field(default="Doe", metadata={'alias': 'lname'})

        @property
        def full_name(self):
            return f"{self.first_name} {self.last_name}"

        @property
        def initials(self):
            return f"{self.first_name[0]}.{self.last_name[0]}."

    model = ModelWithAliasesAndProperties(
        first_name="Alice", last_name="Smith")

    # Test with properties included (default)
    result_with_props = model.as_dict(export_properties=True)

    # Aliased fields should use aliases
    assert "fname" in result_with_props
    assert result_with_props["fname"] == "Alice"
    assert "lname" in result_with_props
    assert result_with_props["lname"] == "Smith"

    # Properties should be included with original names (no aliases for properties)
    assert "full_name" in result_with_props
    assert result_with_props["full_name"] == "Alice Smith"
    assert "initials" in result_with_props
    assert result_with_props["initials"] == "A.S."

    # Test with properties excluded
    result_without_props = model.as_dict(export_properties=False)

    # Only aliased fields should be present
    assert "fname" in result_without_props
    assert "lname" in result_without_props
    assert "full_name" not in result_without_props
    assert "initials" not in result_without_props


def test_field_alias_empty_alias():
    """Test that empty or invalid aliases are ignored"""

    @dataclass
    class ModelWithEmptyAlias(VersionedModel):
        name: str = field(default="test", metadata={
                          'alias': ''})  # Empty alias
        score: int = field(default=100, metadata={'alias': None})  # None alias
        description: str = field(default="desc", metadata={
                                 'alias': 'desc_text'})  # Valid alias

    model = ModelWithEmptyAlias(name="Test", score=95, description="Test desc")
    result = model.as_dict()

    # Empty and None aliases should be ignored, use original field names
    assert "name" in result
    assert result["name"] == "Test"
    assert "score" in result
    assert result["score"] == 95

    # Valid alias should be used
    assert "desc_text" in result
    assert result["desc_text"] == "Test desc"
    assert "description" not in result


def test_field_alias_inheritance():
    """Test field aliases work correctly with model inheritance"""

    @dataclass
    class BaseModelWithAlias(VersionedModel):
        base_field: str = field(default="base", metadata={
                                'alias': 'base_alias'})

    @dataclass
    class DerivedModelWithAlias(BaseModelWithAlias):
        derived_field: str = field(default="derived", metadata={
                                   'alias': 'derived_alias'})
        # Override base field with different alias
        base_field: str = field(default="base", metadata={
                                'alias': 'new_base_alias'})

    model = DerivedModelWithAlias(
        base_field="base_value", derived_field="derived_value")
    result = model.as_dict()

    # Derived class aliases should be used
    assert "new_base_alias" in result
    assert result["new_base_alias"] == "base_value"
    assert "derived_alias" in result
    assert result["derived_alias"] == "derived_value"

    # Original field names and old aliases should not be present
    assert "base_field" not in result
    assert "derived_field" not in result
    assert "base_alias" not in result

    # Test roundtrip
    restored = DerivedModelWithAlias.from_dict(result)
    assert restored.base_field == "base_value"
    assert restored.derived_field == "derived_value"


# Tests for datetime parsing in from_dict()
def test_datetime_parsing_from_iso_string():
    """Test that datetime fields are correctly parsed from ISO strings in from_dict()"""

    @dataclass
    class TestModelWithDatetime(VersionedModel):
        created_at: datetime = field(
            default_factory=lambda: datetime.now(timezone.utc))
        updated_at: datetime = field(
            default_factory=lambda: datetime.now(timezone.utc))

    # Test data with datetime fields as ISO strings (simulating data from MongoDB)
    test_data = {
        'entity_id': 'test123',
        'version': 'version123',
        'previous_version': None,
        'active': True,
        'changed_by_id': 'user123',
        'changed_on': '2024-01-15T10:30:00+00:00',  # ISO string
        'created_at': '2024-01-15T09:00:00+00:00',   # ISO string
        'updated_at': '2024-01-15T11:00:00+00:00',   # ISO string
    }

    # Create model instance from dict
    model = TestModelWithDatetime.from_dict(test_data)

    # Verify that datetime strings were parsed correctly
    assert isinstance(
        model.changed_on, datetime), f"changed_on should be datetime, got {type(model.changed_on)}"
    assert isinstance(
        model.created_at, datetime), f"created_at should be datetime, got {type(model.created_at)}"
    assert isinstance(
        model.updated_at, datetime), f"updated_at should be datetime, got {type(model.updated_at)}"

    # Verify the actual datetime values
    expected_changed_on = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
    expected_created_at = datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc)
    expected_updated_at = datetime(2024, 1, 15, 11, 0, 0, tzinfo=timezone.utc)

    assert model.changed_on == expected_changed_on, f"changed_on mismatch: {model.changed_on} != {expected_changed_on}"
    assert model.created_at == expected_created_at, f"created_at mismatch: {model.created_at} != {expected_created_at}"
    assert model.updated_at == expected_updated_at, f"updated_at mismatch: {model.updated_at} != {expected_updated_at}"


def test_datetime_parsing_optional_fields():
    """Test datetime parsing works with Optional[datetime] fields"""
    from typing import Optional

    @dataclass
    class TestModelWithOptionalDatetime(VersionedModel):
        created_at: datetime = field(
            default_factory=lambda: datetime.now(timezone.utc))
        updated_at: Optional[datetime] = None
        deleted_at: Optional[datetime] = None

    # Test with ISO string for optional field
    test_data = {
        'entity_id': 'test123',
        'created_at': '2024-01-15T09:00:00+00:00',
        'updated_at': '2024-01-15T11:00:00+00:00',  # ISO string
        'deleted_at': None  # None value
    }

    model = TestModelWithOptionalDatetime.from_dict(test_data)

    # Required datetime field should be parsed
    assert isinstance(model.created_at, datetime)
    assert model.created_at == datetime(
        2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc)

    # Optional datetime field with ISO string should be parsed
    assert isinstance(model.updated_at, datetime)
    assert model.updated_at == datetime(
        2024, 1, 15, 11, 0, 0, tzinfo=timezone.utc)

    # Optional datetime field with None should remain None
    assert model.deleted_at is None


def test_datetime_parsing_preserves_existing_datetime():
    """Test that existing datetime objects are preserved unchanged"""

    @dataclass
    class TestModelWithDatetime(VersionedModel):
        created_at: datetime = field(
            default_factory=lambda: datetime.now(timezone.utc))

    # Test with already datetime object
    existing_datetime = datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc)
    test_data = {
        'entity_id': 'test123',
        'created_at': existing_datetime  # Already a datetime object
    }

    model = TestModelWithDatetime.from_dict(test_data)

    # Datetime object should be preserved unchanged
    assert isinstance(model.created_at, datetime)
    assert model.created_at == existing_datetime
    assert model.created_at is existing_datetime  # Should be the same object


def test_datetime_parsing_invalid_strings():
    """Test that invalid datetime strings are left unchanged"""

    @dataclass
    class TestModelWithDatetime(VersionedModel):
        created_at: datetime = field(
            default_factory=lambda: datetime.now(timezone.utc))

    # Test with invalid datetime string
    test_data = {
        'entity_id': 'test123',
        'created_at': 'invalid-date-string'
    }

    model = TestModelWithDatetime.from_dict(test_data)

    # Invalid datetime string should be left as-is
    assert model.created_at == 'invalid-date-string'
    assert isinstance(model.created_at, str)


def test_datetime_parsing_various_iso_formats():
    """Test datetime parsing works with various ISO format strings"""

    @dataclass
    class TestModelWithDatetime(VersionedModel):
        datetime1: datetime = field(
            default_factory=lambda: datetime.now(timezone.utc))
        datetime2: datetime = field(
            default_factory=lambda: datetime.now(timezone.utc))
        datetime3: datetime = field(
            default_factory=lambda: datetime.now(timezone.utc))
        datetime4: datetime = field(
            default_factory=lambda: datetime.now(timezone.utc))

    # Test with various ISO format strings
    test_data = {
        'entity_id': 'test123',
        'datetime1': '2024-01-15T10:30:00Z',           # UTC with Z
        'datetime2': '2024-01-15T10:30:00+00:00',      # UTC with +00:00
        'datetime3': '2024-01-15T10:30:00.123456Z',    # With microseconds
        'datetime4': '2024-01-15T10:30:00-05:00',      # With timezone offset
    }

    model = TestModelWithDatetime.from_dict(test_data)

    # All should be parsed as datetime objects
    assert isinstance(model.datetime1, datetime)
    assert isinstance(model.datetime2, datetime)
    assert isinstance(model.datetime3, datetime)
    assert isinstance(model.datetime4, datetime)

    # Verify specific values
    assert model.datetime1 == datetime(
        2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
    assert model.datetime2 == datetime(
        2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
    assert model.datetime3.year == 2024
    assert model.datetime3.month == 1
    assert model.datetime3.day == 15
    assert model.datetime3.hour == 10
    assert model.datetime3.minute == 30
    assert model.datetime3.second == 0
    assert model.datetime3.microsecond == 123456


def test_datetime_parsing_roundtrip_consistency():
    """Test that datetime parsing maintains consistency through roundtrip conversion"""

    @dataclass
    class TestModelWithDatetime(VersionedModel):
        created_at: datetime = field(
            default_factory=lambda: datetime.now(timezone.utc))
        updated_at: datetime = field(
            default_factory=lambda: datetime.now(timezone.utc))

    # Create original model with datetime objects
    original_created = datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc)
    original_updated = datetime(2024, 1, 15, 11, 30, 45, tzinfo=timezone.utc)

    original = TestModelWithDatetime(
        created_at=original_created,
        updated_at=original_updated
    )

    # Convert to dict with ISO strings
    dict_data = original.as_dict(convert_datetime_to_iso_string=True)

    # Verify datetime fields are converted to ISO strings
    assert isinstance(dict_data['created_at'], str)
    assert isinstance(dict_data['updated_at'], str)

    # Convert back from dict (should parse ISO strings back to datetime)
    restored = TestModelWithDatetime.from_dict(dict_data)

    # Verify datetime objects are correctly restored
    assert isinstance(restored.created_at, datetime)
    assert isinstance(restored.updated_at, datetime)
    assert restored.created_at == original_created
    assert restored.updated_at == original_updated


def test_datetime_parsing_with_other_field_types():
    """Test that datetime parsing works alongside other field type conversions"""
    from enum import Enum
    from typing import Optional

    class Status(Enum):
        active = "active"
        inactive = "inactive"

    @dataclass
    class ComplexModelWithDatetime(VersionedModel):
        status: Optional[Status] = Status.active
        created_at: datetime = field(
            default_factory=lambda: datetime.now(timezone.utc))
        score: int = 100
        name: str = "test"

    # Test data with mixed field types including datetime as ISO string
    test_data = {
        'entity_id': 'test123',
        'status': 'inactive',  # Should become enum
        'created_at': '2024-01-15T09:00:00+00:00',  # Should become datetime
        'score': 95,  # Should remain int
        'name': 'test_model'  # Should remain string
    }

    model = ComplexModelWithDatetime.from_dict(test_data)

    # Verify all field types are correctly converted
    assert model.status == Status.inactive
    assert isinstance(model.status, Status)

    assert isinstance(model.created_at, datetime)
    assert model.created_at == datetime(
        2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc)

    assert model.score == 95
    assert isinstance(model.score, int)

    assert model.name == 'test_model'
    assert isinstance(model.name, str)


def test_datetime_parsing_none_values():
    """Test that None values for datetime fields are handled correctly"""
    from typing import Optional

    @dataclass
    class TestModelWithOptionalDatetime(VersionedModel):
        required_datetime: datetime = field(
            default_factory=lambda: datetime.now(timezone.utc))
        optional_datetime: Optional[datetime] = None

    # Test with None values
    test_data = {
        'entity_id': 'test123',
        'required_datetime': '2024-01-15T09:00:00+00:00',
        'optional_datetime': None
    }

    model = TestModelWithOptionalDatetime.from_dict(test_data)

    # Required datetime should be parsed from string
    assert isinstance(model.required_datetime, datetime)
    assert model.required_datetime == datetime(
        2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc)

    # Optional datetime should remain None
    assert model.optional_datetime is None
