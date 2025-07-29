"""
Test cases for calculated properties fix in VersionedModel
"""

import unittest
from dataclasses import dataclass
from rococo.models.versioned_model import VersionedModel


@dataclass
class TestModelWithCalculatedProperty(VersionedModel):
    """Test model with a calculated property similar to OrganizationImport.error_count"""

    count: int = 0
    errors: list = None

    def __post_init__(self, _is_partial):
        if self.errors is None:
            self.errors = []
        super().__post_init__(_is_partial)

    @property
    def error_count(self) -> int:
        """Calculate error count - this is a read-only property without setter"""
        return len(self.errors) if self.errors else 0


class CalculatedPropertiesFixTestCase(unittest.TestCase):

    def test_setattr_skips_calculated_properties(self):
        """Test that __setattr__ skips calculated properties (properties without setters)"""
        instance = TestModelWithCalculatedProperty(
            count=5, errors=['error1', 'error2'])

        # Verify the calculated property works
        self.assertEqual(instance.error_count, 2)

        # Try to set the calculated property - this should be silently ignored
        setattr(instance, 'error_count', 999)

        # The calculated property should still return the calculated value
        self.assertEqual(instance.error_count, 2)

        # Regular fields should still be settable
        setattr(instance, 'count', 10)
        self.assertEqual(instance.count, 10)

    def test_from_dict_skips_calculated_properties_in_extra_data(self):
        """Test that from_dict skips calculated properties when processing extra data"""
        # Simulate data that includes a calculated property (like from database)
        data = {
            'entity_id': 'test-entity-id',
            'count': 5,
            'errors': ['error1', 'error2'],
            'error_count': 999,  # This should be ignored as it's a calculated property
            'some_extra_field': 'extra_value'  # This should be kept
        }

        # Enable extra fields for this test
        TestModelWithCalculatedProperty.allow_extra = True

        try:
            instance = TestModelWithCalculatedProperty.from_dict(data)

            # The calculated property should return the calculated value, not the value from data
            # len(['error1', 'error2'])
            self.assertEqual(instance.error_count, 2)

            # Regular fields should be set correctly
            self.assertEqual(instance.count, 5)
            self.assertEqual(instance.errors, ['error1', 'error2'])

            # Extra fields should be preserved (except calculated properties)
            self.assertEqual(instance.extra.get(
                'some_extra_field'), 'extra_value')
            self.assertNotIn('error_count', instance.extra)

        finally:
            # Clean up
            TestModelWithCalculatedProperty.allow_extra = False

    def test_as_dict_exports_calculated_properties_when_requested(self):
        """Test that as_dict can export calculated properties when export_properties=True"""
        instance = TestModelWithCalculatedProperty(
            count=5, errors=['error1', 'error2'])

        # With export_properties=True (default), calculated properties should be included
        result = instance.as_dict(export_properties=True)
        self.assertEqual(result['error_count'], 2)

        # With export_properties=False, calculated properties should not be included
        result = instance.as_dict(export_properties=False)
        self.assertNotIn('error_count', result)

    def test_mongodb_repository_scenario(self):
        """Test the specific scenario that was failing in MongoDB repository"""
        instance = TestModelWithCalculatedProperty(
            count=5, errors=['error1', 'error2'])

        # Simulate what happens in MongoDB repository:
        # 1. as_dict() is called with export_properties=True (when save_calculated_fields=True)
        data = instance.as_dict(export_properties=True)
        self.assertEqual(data['error_count'], 2)

        # 2. Data comes back from database and repository tries to set all fields
        # This simulates the problematic code in mongodb_repository.py:
        # for k, v in saved.items():
        #     if hasattr(instance, k):
        #         setattr(instance, k, v)

        for k, v in data.items():
            if hasattr(instance, k):
                # This should not raise an AttributeError for calculated properties
                setattr(instance, k, v)

        # The calculated property should still work correctly
        self.assertEqual(instance.error_count, 2)

    def test_property_with_setter_is_not_skipped(self):
        """Test that properties with setters are not skipped"""

        @dataclass
        class TestModelWithSettableProperty(VersionedModel):
            _internal_value: int = 0

            @property
            def settable_property(self) -> int:
                return self._internal_value

            @settable_property.setter
            def settable_property(self, value: int):
                self._internal_value = value

        instance = TestModelWithSettableProperty()

        # This should work because the property has a setter
        setattr(instance, 'settable_property', 42)
        self.assertEqual(instance.settable_property, 42)
        self.assertEqual(instance._internal_value, 42)


if __name__ == '__main__':
    unittest.main()
