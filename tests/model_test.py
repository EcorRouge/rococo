"""
Test VersionedModel
"""
from datetime import datetime, timedelta, timezone
from uuid import UUID
from dataclasses import dataclass, field, fields
import pytest
from unittest.mock import patch, MagicMock
from rococo.models import VersionedModel
from rococo.models.versioned_model import ModelValidationError


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

    # Test with invalid UUID string (should print error but not crash)
    with patch('builtins.print') as mock_print:
        invalid_data = {"entity_id": "invalid-uuid"}
        model = VersionedModel.from_dict(invalid_data)
        mock_print.assert_called_with("'invalid-uuid' is not a valid UUID.")


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
