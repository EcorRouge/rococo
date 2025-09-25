"""
Test cases for MailjetService address conversion functionality.
"""

import pytest
from rococo.emailing.mailjet import MailjetService


class TestMailjetAddressConversion:
    """Test cases for the _convert_addresses method."""

    def test_string_addresses_conversion(self):
        """Test conversion of string addresses."""
        string_addresses = ["user1@example.com", "user2@example.com"]
        result = MailjetService._convert_addresses(string_addresses)
        expected = [{"Email": "user1@example.com"},
                    {"Email": "user2@example.com"}]
        assert result == expected

    def test_dict_addresses_email_only(self):
        """Test conversion of dict addresses with Email only."""
        dict_addresses = [{"Email": "user1@example.com"},
                          {"Email": "user2@example.com"}]
        result = MailjetService._convert_addresses(dict_addresses)
        expected = [{"Email": "user1@example.com"},
                    {"Email": "user2@example.com"}]
        assert result == expected

    def test_dict_addresses_with_names(self):
        """Test conversion of dict addresses with Email and Name."""
        dict_addresses_with_names = [
            {"Email": "user1@example.com", "Name": "John Doe"},
            {"Email": "user2@example.com", "Name": "Jane Smith"}
        ]
        result = MailjetService._convert_addresses(dict_addresses_with_names)
        expected = [
            {"Email": "user1@example.com", "Name": "John Doe"},
            {"Email": "user2@example.com", "Name": "Jane Smith"}
        ]
        assert result == expected

    def test_mixed_addresses_conversion(self):
        """Test conversion of mixed string and dict addresses."""
        mixed_addresses = [
            "user1@example.com",
            {"Email": "user2@example.com", "Name": "Jane Smith"},
            "user3@example.com"
        ]
        result = MailjetService._convert_addresses(mixed_addresses)
        expected = [
            {"Email": "user1@example.com"},
            {"Email": "user2@example.com", "Name": "Jane Smith"},
            {"Email": "user3@example.com"}
        ]
        assert result == expected

    def test_empty_list_conversion(self):
        """Test conversion of empty address list."""
        empty_addresses = []
        result = MailjetService._convert_addresses(empty_addresses)
        expected = []
        assert result == expected

    def test_dict_without_email_key_raises_error(self):
        """Test that dict without Email key raises ValueError."""
        invalid_dict = [{"Name": "John Doe"}]  # Missing Email key
        with pytest.raises(ValueError, match="Dict address must contain 'Email' key"):
            MailjetService._convert_addresses(invalid_dict)

    def test_invalid_type_raises_error(self):
        """Test that invalid address type raises TypeError."""
        invalid_type = [123]  # Invalid type
        with pytest.raises(TypeError, match="Address must be string or dict"):
            MailjetService._convert_addresses(invalid_type)

    def test_dict_with_additional_fields(self):
        """Test that dict addresses with additional fields are preserved."""
        addresses_with_extra = [
            {"Email": "user@example.com", "Name": "John", "CustomField": "value"}
        ]
        result = MailjetService._convert_addresses(addresses_with_extra)
        expected = [
            {"Email": "user@example.com", "Name": "John", "CustomField": "value"}
        ]
        assert result == expected
