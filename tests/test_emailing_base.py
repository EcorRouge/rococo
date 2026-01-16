"""
Tests for email abstract base class.

This module tests the abstract EmailService base class that defines
the interface contract for email service providers.
"""
import unittest
from abc import ABC

from rococo.emailing.base import EmailService
from rococo.emailing.config import EmailConfig


class TestEmailServiceAbstractClass(unittest.TestCase):
    """Test EmailService abstract base class."""

    def test_email_service_is_abstract(self):
        """
        Test that EmailService is an abstract base class.

        Verifies it inherits from ABC.
        """
        self.assertTrue(issubclass(EmailService, ABC))

    def test_email_service_cannot_be_instantiated_directly(self):
        """
        Test that EmailService cannot be instantiated without implementing abstract methods.

        Verifies abstract class enforcement.
        """
        with self.assertRaises(TypeError) as context:
            EmailService()

        self.assertIn("abstract", str(context.exception).lower())

    def test_email_service_has_abstract_methods_defined(self):
        """
        Test that EmailService has required abstract methods defined.

        Verifies the interface contract includes:
        - __init__
        - __call__
        - send_email
        - create_contact
        - remove_contact
        """
        abstract_methods = EmailService.__abstractmethods__
        self.assertIn('__init__', abstract_methods)
        self.assertIn('__call__', abstract_methods)
        self.assertIn('send_email', abstract_methods)
        self.assertIn('create_contact', abstract_methods)
        self.assertIn('remove_contact', abstract_methods)

    def test_email_service_can_be_subclassed_with_all_methods(self):
        """
        Test that EmailService can be subclassed when all abstract methods are implemented.

        Verifies complete implementation works.
        """
        class CompleteEmailService(EmailService):
            def __init__(self):
                pass  # Minimal implementation for test purposes; no initialization needed

            def __call__(self, config: EmailConfig, *args, **kwargs):
                self.config = config
                return self

            def send_email(self, message: dict):
                return {"status": "sent"}

            def create_contact(self, email: str, name: str, list_id: str, extra: dict):
                return {"contact_id": 123}

            def remove_contact(self, email: str):
                return True

        # Should not raise
        service = CompleteEmailService()
        self.assertIsInstance(service, EmailService)

    def test_incomplete_email_service_cannot_be_instantiated(self):
        """
        Test that incomplete EmailService subclasses cannot be instantiated.

        Verifies abstract method enforcement.
        """
        class IncompleteEmailService(EmailService):
            def __init__(self):
                # Intentionally minimal implementation for testing abstract method enforcement
                pass

            def __call__(self, config: EmailConfig, *args, **kwargs):
                self.config = config

            # Missing send_email, create_contact, remove_contact

        with self.assertRaises(TypeError) as context:
            IncompleteEmailService()

        self.assertIn("abstract", str(context.exception).lower())

    def test_email_service_config_attribute_is_typed(self):
        """
        Test that EmailService has a typed config attribute.

        Verifies config attribute is declared with type annotation.
        """
        self.assertTrue(hasattr(EmailService, '__annotations__'))
        self.assertIn('config', EmailService.__annotations__)
        self.assertEqual(EmailService.__annotations__['config'], EmailConfig)


if __name__ == '__main__':
    unittest.main()
