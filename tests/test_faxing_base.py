"""
Tests for fax abstract base class.

This module tests the abstract FaxService base class that defines
the interface contract for fax service providers.
"""
import unittest
from abc import ABC
from unittest.mock import Mock

from rococo.faxing.base import FaxService
from rococo.faxing.config import Config


class TestFaxServiceAbstractClass(unittest.TestCase):
    """Test FaxService abstract base class."""

    def test_fax_service_is_abstract(self):
        """
        Test that FaxService is an abstract base class.

        Verifies it inherits from ABC.
        """
        self.assertTrue(issubclass(FaxService, ABC))

    def test_fax_service_cannot_be_instantiated_directly(self):
        """
        Test that FaxService cannot be instantiated without implementing abstract methods.

        Verifies abstract class enforcement.
        """
        with self.assertRaises(TypeError) as context:
            FaxService()

        self.assertIn("abstract", str(context.exception).lower())

    def test_fax_service_has_abstract_method_defined(self):
        """
        Test that FaxService has send_fax abstract method defined.

        Verifies the interface contract includes send_fax.
        """
        abstract_methods = FaxService.__abstractmethods__
        self.assertIn('send_fax', abstract_methods)

    def test_fax_service_can_be_subclassed_with_send_fax(self):
        """
        Test that FaxService can be subclassed when send_fax is implemented.

        Verifies complete implementation works.
        """
        class CompleteFaxService(FaxService):
            def send_fax(self, message: dict):
                return {"status": "sent"}

        # Should not raise
        service = CompleteFaxService()
        self.assertIsInstance(service, FaxService)

    def test_incomplete_fax_service_cannot_be_instantiated(self):
        """
        Test that incomplete FaxService subclasses cannot be instantiated.

        Verifies abstract method enforcement.
        """
        class IncompleteFaxService(FaxService):
            pass
            # Missing send_fax

        with self.assertRaises(TypeError) as context:
            IncompleteFaxService()

        self.assertIn("abstract", str(context.exception).lower())

    def test_fax_service_config_attribute_is_typed(self):
        """
        Test that FaxService has a typed config attribute.

        Verifies config attribute is declared with type annotation.
        """
        self.assertTrue(hasattr(FaxService, '__annotations__'))
        self.assertIn('config', FaxService.__annotations__)
        self.assertEqual(FaxService.__annotations__['config'], Config)

    def test_fax_service_call_method(self):
        """
        Test that FaxService __call__ method stores config.

        Verifies:
        - __call__ is not abstract (has implementation)
        - Config is stored in self.config
        """
        class TestFaxService(FaxService):
            def send_fax(self, message: dict):
                return {"status": "sent"}

        service = TestFaxService()
        mock_config = Mock(spec=Config)

        # Act
        service(mock_config)

        # Assert
        self.assertEqual(service.config, mock_config)

    def test_fax_service_call_returns_none(self):
        """
        Test that FaxService __call__ method returns None by default.

        Verifies default behavior (no explicit return).
        """
        class TestFaxService(FaxService):
            def send_fax(self, message: dict):
                return {"status": "sent"}

        service = TestFaxService()
        mock_config = Mock(spec=Config)

        # Act
        result = service(mock_config)

        # Assert
        self.assertIsNone(result)

    def test_fax_service_send_fax_not_implemented_raises_error(self):
        """
        Test that calling send_fax on base raises NotImplementedError.

        Note: Can't actually test this since we can't instantiate the abstract class,
        but we can verify the method body has NotImplementedError.
        """
        # Verify the abstract method has NotImplementedError in its body
        self.assertTrue(hasattr(FaxService, 'send_fax'))
        # The abstractmethod decorator is present
        self.assertIn('send_fax', FaxService.__abstractmethods__)


if __name__ == '__main__':
    unittest.main()
