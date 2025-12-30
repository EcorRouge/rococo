"""
Tests for abstract messaging base classes.

This module tests the abstract base classes that define the
messaging adapter and service processor interfaces.

Note: These classes use @abstractmethod decorators but don't inherit from ABC,
so they don't enforce abstract methods at instantiation time. However, they
define the interface contract for subclasses.
"""
import unittest

from rococo.messaging.base import MessageAdapter, BaseServiceProcessor


class TestMessageAdapter(unittest.TestCase):
    """Test MessageAdapter abstract class."""

    def test_message_adapter_can_be_instantiated_directly(self):
        """
        Test that MessageAdapter can be instantiated (doesn't inherit from ABC).

        Note: While it has @abstractmethod decorators, it doesn't inherit from ABC,
        so Python doesn't enforce the abstract methods.
        """
        # Can be instantiated despite @abstractmethod decorators
        adapter = MessageAdapter()
        self.assertIsInstance(adapter, MessageAdapter)

    def test_message_adapter_has_abstract_methods_defined(self):
        """
        Test that MessageAdapter has abstract methods defined.

        Verifies the interface contract is defined.
        """
        # Check that the methods are defined
        self.assertTrue(hasattr(MessageAdapter, 'send_message'))
        self.assertTrue(hasattr(MessageAdapter, 'consume_messages'))
        self.assertTrue(hasattr(MessageAdapter, '__enter__'))
        self.assertTrue(hasattr(MessageAdapter, '__exit__'))

    def test_message_adapter_can_be_instantiated_with_all_methods(self):
        """
        Test that MessageAdapter can be instantiated when all abstract methods are implemented.

        Verifies complete implementation works.
        """
        class CompleteAdapter(MessageAdapter):
            def send_message(self, queue_name: str, message: dict):
                # pass minimal implementation for testing abstract method enforcement
                pass

            def consume_messages(self, queue_name: str, callback_function: callable = None):
                # pass minimal implementation for testing abstract method enforcement
                pass

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                # pass minimal implementation for testing abstract method enforcement
                pass

        # Should not raise
        adapter = CompleteAdapter()
        self.assertIsInstance(adapter, MessageAdapter)

    def test_message_adapter_context_manager_works(self):
        """
        Test that MessageAdapter subclass works as context manager.

        Verifies context manager protocol functionality.
        """
        class TestAdapter(MessageAdapter):
            def send_message(self, queue_name: str, message: dict):
                self.sent_message = message

            def consume_messages(self, queue_name: str, callback_function: callable = None):
                # pass minimal implementation for testing abstract method enforcement
                pass

            def __enter__(self):
                self.entered = True
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                self.exited = True

        with TestAdapter() as adapter:
            self.assertTrue(adapter.entered)
            adapter.send_message("test", {"data": "value"})

        self.assertTrue(adapter.exited)
        self.assertEqual(adapter.sent_message, {"data": "value"})


class TestBaseServiceProcessor(unittest.TestCase):
    """Test BaseServiceProcessor abstract class."""

    def test_base_service_processor_can_be_instantiated_directly(self):
        """
        Test that BaseServiceProcessor can be instantiated (doesn't inherit from ABC).

        Note: While it has @abstractmethod decorator, it doesn't inherit from ABC,
        so Python doesn't enforce the abstract method.
        """
        # Can be instantiated despite @abstractmethod decorator
        processor = BaseServiceProcessor()
        self.assertIsInstance(processor, BaseServiceProcessor)

    def test_base_service_processor_has_abstract_method_defined(self):
        """
        Test that BaseServiceProcessor has process method defined.

        Verifies the interface contract is defined.
        """
        # Check that the method is defined
        self.assertTrue(hasattr(BaseServiceProcessor, 'process'))

    def test_base_service_processor_can_be_instantiated_with_process(self):
        """
        Test that BaseServiceProcessor can be instantiated when process is implemented.

        Verifies complete implementation works.
        """
        class CompleteProcessor(BaseServiceProcessor):
            def process(self, message):
                return message

        # Should not raise
        processor = CompleteProcessor()
        self.assertIsInstance(processor, BaseServiceProcessor)

    def test_base_service_processor_process_method_works(self):
        """
        Test that BaseServiceProcessor subclass process method works.

        Verifies process method functionality.
        """
        class TestProcessor(BaseServiceProcessor):
            def process(self, message):
                return {"processed": True, "original": message}

        processor = TestProcessor()
        result = processor.process({"data": "test"})

        self.assertEqual(result, {"processed": True, "original": {"data": "test"}})


if __name__ == '__main__':
    unittest.main()
