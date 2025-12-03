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
        with pytest.raises(ValueError, match="Dict address must contain 'Email' or 'email' key"):
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

    def test_lowercase_keys_with_name(self):
        """Test conversion of dict addresses with lowercase 'email' and 'name' keys."""
        lowercase_addresses = [
            {"email": "user1@example.com", "name": "John Doe"},
            {"email": "user2@example.com", "name": "Jane Smith"}
        ]
        result = MailjetService._convert_addresses(lowercase_addresses)
        expected = [
            {"Email": "user1@example.com", "Name": "John Doe"},
            {"Email": "user2@example.com", "Name": "Jane Smith"}
        ]
        assert result == expected

    def test_lowercase_keys_without_name(self):
        """Test conversion of dict addresses with lowercase 'email' key only."""
        lowercase_addresses = [{"email": "user@example.com"}]
        result = MailjetService._convert_addresses(lowercase_addresses)
        expected = [{"Email": "user@example.com"}]
        assert result == expected

    def test_mixed_case_keys(self):
        """Test conversion of mixed uppercase and lowercase keys in same list."""
        mixed_addresses = [
            {"Email": "user1@example.com", "Name": "John"},
            {"email": "user2@example.com", "name": "Jane"},
            "user3@example.com"
        ]
        result = MailjetService._convert_addresses(mixed_addresses)
        expected = [
            {"Email": "user1@example.com", "Name": "John"},
            {"Email": "user2@example.com", "Name": "Jane"},
            {"Email": "user3@example.com"}
        ]
        assert result == expected

    def test_none_name_value(self):
        """Test that None name values are omitted from output."""
        addresses_with_none = [
            {"Email": "user1@example.com", "Name": None},
            {"email": "user2@example.com", "name": None}
        ]
        result = MailjetService._convert_addresses(addresses_with_none)
        expected = [
            {"Email": "user1@example.com"},
            {"Email": "user2@example.com"}
        ]
        assert result == expected

    def test_empty_string_name_value(self):
        """Test that empty string name values are omitted from output."""
        addresses_with_empty = [
            {"Email": "user1@example.com", "Name": ""},
            {"email": "user2@example.com", "name": ""}
        ]
        result = MailjetService._convert_addresses(addresses_with_empty)
        expected = [
            {"Email": "user1@example.com"},
            {"Email": "user2@example.com"}
        ]
        assert result == expected

    def test_whitespace_only_name_value(self):
        """Test that whitespace-only name values are omitted from output."""
        addresses_with_whitespace = [
            {"Email": "user1@example.com", "Name": "   "},
            {"email": "user2@example.com", "name": "\t\n"},
            {"Email": "user3@example.com", "Name": " "}
        ]
        result = MailjetService._convert_addresses(addresses_with_whitespace)
        expected = [
            {"Email": "user1@example.com"},
            {"Email": "user2@example.com"},
            {"Email": "user3@example.com"}
        ]
        assert result == expected

    def test_name_with_surrounding_whitespace(self):
        """Test that names with surrounding whitespace are preserved."""
        addresses_with_whitespace = [
            {"Email": "user@example.com", "Name": "  John Doe  "}
        ]
        result = MailjetService._convert_addresses(addresses_with_whitespace)
        expected = [
            {"Email": "user@example.com", "Name": "  John Doe  "}
        ]
        assert result == expected

    def test_lowercase_email_key_missing_raises_error(self):
        """Test that dict without email or Email key raises ValueError."""
        invalid_dict = [{"name": "John Doe"}]  # Missing both Email and email keys
        with pytest.raises(ValueError, match="Dict address must contain 'Email' or 'email' key"):
            MailjetService._convert_addresses(invalid_dict)

    def test_mixed_valid_and_none_names(self):
        """Test conversion of addresses with mixed valid and None/empty names."""
        mixed_names = [
            {"Email": "user1@example.com", "Name": "John Doe"},
            {"Email": "user2@example.com", "Name": None},
            {"email": "user3@example.com", "name": ""},
            {"Email": "user4@example.com", "Name": "Jane Smith"}
        ]
        result = MailjetService._convert_addresses(mixed_names)
        expected = [
            {"Email": "user1@example.com", "Name": "John Doe"},
            {"Email": "user2@example.com"},
            {"Email": "user3@example.com"},
            {"Email": "user4@example.com", "Name": "Jane Smith"}
        ]
        assert result == expected

    def test_lowercase_dict_with_additional_fields(self):
        """Test that lowercase dict addresses with additional fields are preserved."""
        addresses_with_extra = [
            {"email": "user@example.com", "name": "John", "CustomField": "value"}
        ]
        result = MailjetService._convert_addresses(addresses_with_extra)
        expected = [
            {"Email": "user@example.com", "Name": "John", "CustomField": "value"}
        ]
        assert result == expected

    def test_non_string_name_values(self):
        """Test that non-string name values (like integers) are omitted."""
        addresses_with_non_string = [
            {"Email": "user1@example.com", "Name": 123},
            {"email": "user2@example.com", "name": []},
            {"Email": "user3@example.com", "Name": {}}
        ]
        result = MailjetService._convert_addresses(addresses_with_non_string)
        expected = [
            {"Email": "user1@example.com"},
            {"Email": "user2@example.com"},
            {"Email": "user3@example.com"}
        ]
        assert result == expected

    def test_both_email_keys_uppercase_takes_precedence(self):
        """Test that uppercase 'Email' takes precedence when both keys are present."""
        addresses_with_both = [
            {"Email": "uppercase@example.com", "email": "lowercase@example.com"},
            {"Email": "primary@example.com", "email": "secondary@example.com", "Name": "John"}
        ]
        result = MailjetService._convert_addresses(addresses_with_both)
        expected = [
            {"Email": "uppercase@example.com"},
            {"Email": "primary@example.com", "Name": "John"}
        ]
        assert result == expected

    def test_both_name_keys_uppercase_takes_precedence(self):
        """Test that uppercase 'Name' takes precedence when both keys are present."""
        addresses_with_both = [
            {"Email": "user@example.com", "Name": "Uppercase Name", "name": "lowercase name"}
        ]
        result = MailjetService._convert_addresses(addresses_with_both)
        expected = [
            {"Email": "user@example.com", "Name": "Uppercase Name"}
        ]
        assert result == expected

    def test_empty_email_falls_back_to_lowercase(self):
        """Test that empty 'Email' string falls back to lowercase 'email' key."""
        addresses_with_fallback = [
            {"Email": "", "email": "fallback@example.com"},
            {"Email": "", "email": "another@example.com", "name": "Jane"}
        ]
        result = MailjetService._convert_addresses(addresses_with_fallback)
        expected = [
            {"Email": "fallback@example.com"},
            {"Email": "another@example.com", "Name": "Jane"}
        ]
        assert result == expected

    # =====================================================
    # Invalid Input Edge Cases
    # =====================================================

    def test_empty_dict_raises_error(self):
        """Test that empty dict raises ValueError."""
        empty_dict = [{}]
        with pytest.raises(ValueError, match="Dict address must contain 'Email' or 'email' key"):
            MailjetService._convert_addresses(empty_dict)

    def test_none_in_list_raises_error(self):
        """Test that None in address list raises TypeError."""
        none_in_list = [None]
        with pytest.raises(TypeError, match="Address must be string or dict"):
            MailjetService._convert_addresses(none_in_list)

    def test_boolean_true_raises_error(self):
        """Test that boolean True raises TypeError."""
        boolean_true = [True]
        with pytest.raises(TypeError, match="Address must be string or dict"):
            MailjetService._convert_addresses(boolean_true)

    def test_boolean_false_raises_error(self):
        """Test that boolean False raises TypeError."""
        boolean_false = [False]
        with pytest.raises(TypeError, match="Address must be string or dict"):
            MailjetService._convert_addresses(boolean_false)

    def test_tuple_raises_error(self):
        """Test that tuple input raises TypeError."""
        tuple_input = [("user@example.com", "John Doe")]
        with pytest.raises(TypeError, match="Address must be string or dict"):
            MailjetService._convert_addresses(tuple_input)

    # =====================================================
    # Fallback Behavior Tests
    # =====================================================

    def test_empty_name_falls_back_to_lowercase(self):
        """Test that empty 'Name' string falls back to lowercase 'name' key."""
        addresses_with_fallback = [
            {"Email": "user@example.com", "Name": "", "name": "Valid Name"}
        ]
        result = MailjetService._convert_addresses(addresses_with_fallback)
        expected = [
            {"Email": "user@example.com", "Name": "Valid Name"}
        ]
        assert result == expected

    # =====================================================
    # Special Character and Unicode Tests
    # =====================================================

    def test_special_characters_in_name(self):
        """Test that special characters in name are preserved."""
        addresses_with_special = [
            {"Email": "user@example.com", "Name": "O'Brien & Co."}
        ]
        result = MailjetService._convert_addresses(addresses_with_special)
        expected = [
            {"Email": "user@example.com", "Name": "O'Brien & Co."}
        ]
        assert result == expected

    def test_unicode_in_name(self):
        """Test that unicode characters in name are preserved."""
        addresses_with_unicode = [
            {"Email": "user@example.com", "Name": "José García"}
        ]
        result = MailjetService._convert_addresses(addresses_with_unicode)
        expected = [
            {"Email": "user@example.com", "Name": "José García"}
        ]
        assert result == expected

    def test_unicode_in_email(self):
        """Test that unicode characters in email are preserved (Mailjet handles validation)."""
        addresses_with_unicode = [
            {"Email": "用户@例子.中国"}
        ]
        result = MailjetService._convert_addresses(addresses_with_unicode)
        expected = [
            {"Email": "用户@例子.中国"}
        ]
        assert result == expected

    # =====================================================
    # Complex Additional Fields Tests
    # =====================================================

    def test_nested_dict_as_additional_field(self):
        """Test that nested dict as additional field is preserved."""
        addresses_with_nested = [
            {"Email": "user@example.com", "Name": "John", "Metadata": {"key": "value", "nested": {"deep": True}}}
        ]
        result = MailjetService._convert_addresses(addresses_with_nested)
        expected = [
            {"Email": "user@example.com", "Name": "John", "Metadata": {"key": "value", "nested": {"deep": True}}}
        ]
        assert result == expected

    def test_list_as_additional_field(self):
        """Test that list as additional field is preserved."""
        addresses_with_list = [
            {"Email": "user@example.com", "Name": "John", "Tags": ["important", "vip", "newsletter"]}
        ]
        result = MailjetService._convert_addresses(addresses_with_list)
        expected = [
            {"Email": "user@example.com", "Name": "John", "Tags": ["important", "vip", "newsletter"]}
        ]
        assert result == expected

    # =====================================================
    # Potential Bug Detection Tests
    # =====================================================

    def test_whitespace_only_email_passes_through(self):
        """
        Test that whitespace-only email passes through without validation.
        
        Note: This documents current behavior. Whitespace-only emails are truthy
        strings, so they pass the `if not email:` check. Mailjet API will handle
        the validation and return an appropriate error.
        """
        addresses_with_whitespace_email = [
            {"Email": "   "}
        ]
        result = MailjetService._convert_addresses(addresses_with_whitespace_email)
        expected = [
            {"Email": "   "}
        ]
        assert result == expected
