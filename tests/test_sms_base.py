"""
Tests for SMS abstract base class.

This module tests the abstract SMSService base class that defines
the interface contract for SMS service providers.
"""
import unittest
from abc import ABC

from rococo.sms.base import SMSService
from rococo.sms.config import SMSConfig


class TestSMSServiceAbstractClass(unittest.TestCase):
    """Test SMSService abstract base class."""

    def test_sms_service_is_abstract(self):
        """
        Test that SMSService is an abstract base class.

        Verifies it inherits from ABC.
        """
        self.assertTrue(issubclass(SMSService, ABC))

    def test_sms_service_cannot_be_instantiated_directly(self):
        """
        Test that SMSService cannot be instantiated without implementing abstract methods.

        Verifies abstract class enforcement.
        """
        with self.assertRaises(TypeError) as context:
            SMSService()

        self.assertIn("abstract", str(context.exception).lower())

    def test_sms_service_has_abstract_methods_defined(self):
        """
        Test that SMSService has required abstract methods defined.

        Verifies the interface contract includes:
        - __init__
        - __call__
        - send_sms
        """
        abstract_methods = SMSService.__abstractmethods__
        self.assertIn('__init__', abstract_methods)
        self.assertIn('__call__', abstract_methods)
        self.assertIn('send_sms', abstract_methods)

    def test_sms_service_can_be_subclassed_with_all_methods(self):
        """
        Test that SMSService can be subclassed when all abstract methods are implemented.

        Verifies complete implementation works.
        """
        class CompleteSMSService(SMSService):
            def __init__(self):
                # pass minimal implementation for testing abstract method enforcement
                pass

            def __call__(self, config: SMSConfig, *args, **kwargs):
                self.config = config
                return self

            def send_sms(self, event_name: str, phone_number: str, parameters: dict):
                return {"status": "sent"}

        # Should not raise
        service = CompleteSMSService()
        self.assertIsInstance(service, SMSService)

    def test_incomplete_sms_service_cannot_be_instantiated(self):
        """
        Test that incomplete SMSService subclasses cannot be instantiated.

        Verifies abstract method enforcement.
        """
        class IncompleteSMSService(SMSService):
            def __init__(self):
                pass

            # Missing __call__ and send_sms

        with self.assertRaises(TypeError) as context:
            IncompleteSMSService()

        self.assertIn("abstract", str(context.exception).lower())

    def test_sms_service_config_attribute_is_typed(self):
        """
        Test that SMSService has a typed config attribute.

        Verifies config attribute is declared with type annotation.
        """
        self.assertTrue(hasattr(SMSService, '__annotations__'))
        self.assertIn('config', SMSService.__annotations__)
        self.assertEqual(SMSService.__annotations__['config'], SMSConfig)


if __name__ == '__main__':
    unittest.main()
