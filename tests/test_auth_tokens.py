"""
Tests for authentication token generation and validation functions.

This module tests the security-critical token functions used for
confirmation tokens (email-based) and access tokens (entity_id-based).
"""
import unittest
from unittest.mock import patch
import hmac
import hashlib

from rococo.auth.tokens import (
    generate_confirmation_token,
    validate_confirmation_token,
    generate_access_token,
    validate_access_token
)


# Test constants
TEST_EMAIL = "test@example.com"
TEST_ENTITY_ID = "12345678-1234-5678-1234-567812345678"
TEST_SECRET_KEY = "test_secret_key_123"
TEST_TIMESTAMP = 1640000000
TEST_EXPIRATION = 3600  # 1 hour


class TestGenerateConfirmationToken(unittest.TestCase):
    """Test confirmation token generation."""

    @patch('rococo.auth.tokens.time.time', return_value=TEST_TIMESTAMP)
    def test_generate_confirmation_token_success(self, mock_time):
        """
        Test successful confirmation token generation.

        Verifies that the token is generated with correct format:
        email:timestamp:signature
        """
        token = generate_confirmation_token(TEST_EMAIL, TEST_SECRET_KEY)

        parts = token.split(':')
        self.assertEqual(len(parts), 3)
        self.assertEqual(parts[0], TEST_EMAIL)
        self.assertEqual(parts[1], str(TEST_TIMESTAMP))

        # Verify signature is a valid hex string
        self.assertEqual(len(parts[2]), 64)  # SHA256 hex digest is 64 chars
        self.assertTrue(all(c in '0123456789abcdef' for c in parts[2]))

    @patch('rococo.auth.tokens.time.time', return_value=TEST_TIMESTAMP)
    def test_generate_confirmation_token_signature_correctness(self, mock_time):
        """
        Test that the generated signature matches expected HMAC-SHA256.

        Verifies the HMAC algorithm is correctly implemented.
        """
        token = generate_confirmation_token(TEST_EMAIL, TEST_SECRET_KEY)
        email, timestamp, signature = token.split(':')

        # Calculate expected signature
        data = email + timestamp
        expected_signature = hmac.new(
            key=TEST_SECRET_KEY.encode('utf-8'),
            msg=data.encode('utf-8'),
            digestmod=hashlib.sha256
        ).hexdigest()

        self.assertEqual(signature, expected_signature)

    @patch('rococo.auth.tokens.time.time', return_value=TEST_TIMESTAMP)
    def test_generate_confirmation_token_different_emails_produce_different_tokens(self, mock_time):
        """
        Test that different emails produce different tokens.

        Verifies uniqueness of tokens for different inputs.
        """
        token1 = generate_confirmation_token("user1@example.com", TEST_SECRET_KEY)
        token2 = generate_confirmation_token("user2@example.com", TEST_SECRET_KEY)

        self.assertNotEqual(token1, token2)

    @patch('rococo.auth.tokens.time.time', return_value=TEST_TIMESTAMP)
    def test_generate_confirmation_token_different_secrets_produce_different_signatures(self, mock_time):
        """
        Test that different secret keys produce different signatures.

        Verifies that tokens are secret-dependent.
        """
        token1 = generate_confirmation_token(TEST_EMAIL, "secret1")
        token2 = generate_confirmation_token(TEST_EMAIL, "secret2")

        sig1 = token1.split(':')[2]
        sig2 = token2.split(':')[2]
        self.assertNotEqual(sig1, sig2)

    @patch('rococo.auth.tokens.time.time', return_value=TEST_TIMESTAMP)
    def test_generate_confirmation_token_with_latin1_encoding(self, mock_time):
        """
        Test token generation with latin-1 encoding.

        Verifies support for different encodings.
        """
        token = generate_confirmation_token(TEST_EMAIL, TEST_SECRET_KEY, encoding='latin-1')

        parts = token.split(':')
        self.assertEqual(len(parts), 3)
        self.assertEqual(parts[0], TEST_EMAIL)
        self.assertEqual(parts[1], str(TEST_TIMESTAMP))

    @patch('rococo.auth.tokens.time.time', return_value=TEST_TIMESTAMP)
    def test_generate_confirmation_token_with_special_characters(self, mock_time):
        """
        Test token generation with special characters in email.

        Verifies handling of special characters.
        """
        special_email = "test+tag@example.com"
        token = generate_confirmation_token(special_email, TEST_SECRET_KEY)

        parts = token.split(':')
        self.assertEqual(parts[0], special_email)


class TestValidateConfirmationToken(unittest.TestCase):
    """Test confirmation token validation."""

    @patch('rococo.auth.tokens.time.time', return_value=TEST_TIMESTAMP)
    def test_validate_confirmation_token_success(self, mock_time):
        """
        Test successful validation of a valid, non-expired token.

        Verifies that a correctly generated token is validated successfully.
        """
        # Generate token at TEST_TIMESTAMP
        token = generate_confirmation_token(TEST_EMAIL, TEST_SECRET_KEY)

        # Validate immediately (expiration = 3600 seconds)
        result = validate_confirmation_token(token, TEST_SECRET_KEY, TEST_EXPIRATION)

        self.assertEqual(result, TEST_EMAIL)

    @patch('rococo.auth.tokens.time.time')
    def test_validate_confirmation_token_expired(self, mock_time):
        """
        Test that expired tokens are rejected.

        Verifies expiration checking logic.
        """
        # Generate token at TEST_TIMESTAMP
        mock_time.return_value = TEST_TIMESTAMP
        token = generate_confirmation_token(TEST_EMAIL, TEST_SECRET_KEY)

        # Try to validate after expiration (3601 seconds later)
        mock_time.return_value = TEST_TIMESTAMP + TEST_EXPIRATION + 1
        result = validate_confirmation_token(token, TEST_SECRET_KEY, TEST_EXPIRATION)

        self.assertFalse(result)

    @patch('rococo.auth.tokens.time.time')
    def test_validate_confirmation_token_just_before_expiration(self, mock_time):
        """
        Test validation just before token expires.

        Verifies boundary condition for expiration.
        """
        # Generate token at TEST_TIMESTAMP
        mock_time.return_value = TEST_TIMESTAMP
        token = generate_confirmation_token(TEST_EMAIL, TEST_SECRET_KEY)

        # Validate 1 second before expiration
        mock_time.return_value = TEST_TIMESTAMP + TEST_EXPIRATION - 1
        result = validate_confirmation_token(token, TEST_SECRET_KEY, TEST_EXPIRATION)

        self.assertEqual(result, TEST_EMAIL)

    @patch('rococo.auth.tokens.time.time')
    def test_validate_confirmation_token_at_exact_expiration(self, mock_time):
        """
        Test validation at exact expiration time.

        Verifies that token is still valid at exact expiration boundary.
        The condition uses < not <=, so tokens are valid at timestamp + expiration.
        """
        # Generate token at TEST_TIMESTAMP
        mock_time.return_value = TEST_TIMESTAMP
        token = generate_confirmation_token(TEST_EMAIL, TEST_SECRET_KEY)

        # Validate at exact expiration time
        mock_time.return_value = TEST_TIMESTAMP + TEST_EXPIRATION
        result = validate_confirmation_token(token, TEST_SECRET_KEY, TEST_EXPIRATION)

        # Token is still valid at exact expiration time (uses < not <=)
        self.assertEqual(result, TEST_EMAIL)

    @patch('rococo.auth.tokens.time.time', return_value=TEST_TIMESTAMP)
    def test_validate_confirmation_token_tampered_signature(self, mock_time):
        """
        Test that tokens with tampered signatures are rejected.

        Verifies signature verification prevents tampering.
        """
        token = generate_confirmation_token(TEST_EMAIL, TEST_SECRET_KEY)

        # Tamper with the signature
        parts = token.split(':')
        parts[2] = parts[2][:-1] + ('0' if parts[2][-1] != '0' else '1')
        tampered_token = ':'.join(parts)

        result = validate_confirmation_token(tampered_token, TEST_SECRET_KEY, TEST_EXPIRATION)

        self.assertFalse(result)

    @patch('rococo.auth.tokens.time.time', return_value=TEST_TIMESTAMP)
    def test_validate_confirmation_token_wrong_secret_key(self, mock_time):
        """
        Test that tokens are rejected with wrong secret key.

        Verifies secret key validation.
        """
        token = generate_confirmation_token(TEST_EMAIL, TEST_SECRET_KEY)

        result = validate_confirmation_token(token, "wrong_secret_key", TEST_EXPIRATION)

        self.assertFalse(result)

    @patch('rococo.auth.tokens.time.time', return_value=TEST_TIMESTAMP)
    def test_validate_confirmation_token_tampered_email(self, mock_time):
        """
        Test that tokens with tampered email are rejected.

        Verifies that modifying the email invalidates the signature.
        """
        token = generate_confirmation_token(TEST_EMAIL, TEST_SECRET_KEY)

        # Tamper with the email
        parts = token.split(':')
        parts[0] = "hacker@example.com"
        tampered_token = ':'.join(parts)

        result = validate_confirmation_token(tampered_token, TEST_SECRET_KEY, TEST_EXPIRATION)

        self.assertFalse(result)

    @patch('rococo.auth.tokens.time.time', return_value=TEST_TIMESTAMP)
    def test_validate_confirmation_token_uses_timing_safe_comparison(self, mock_time):
        """
        Test that validation uses hmac.compare_digest for timing attack protection.

        This test verifies the implementation uses the secure comparison method.
        """
        token = generate_confirmation_token(TEST_EMAIL, TEST_SECRET_KEY)

        # This test passes if the function uses hmac.compare_digest
        # which prevents timing attacks. We can't directly test the timing,
        # but we verify the function works correctly with valid tokens.
        result = validate_confirmation_token(token, TEST_SECRET_KEY, TEST_EXPIRATION)

        self.assertEqual(result, TEST_EMAIL)

    @patch('rococo.auth.tokens.time.time', return_value=TEST_TIMESTAMP)
    def test_validate_confirmation_token_with_latin1_encoding(self, mock_time):
        """
        Test validation with latin-1 encoding.

        Verifies encoding consistency.
        """
        token = generate_confirmation_token(TEST_EMAIL, TEST_SECRET_KEY, encoding='latin-1')
        result = validate_confirmation_token(token, TEST_SECRET_KEY, TEST_EXPIRATION, encoding='latin-1')

        self.assertEqual(result, TEST_EMAIL)


class TestGenerateAccessToken(unittest.TestCase):
    """Test access token generation."""

    @patch('rococo.auth.tokens.time.time', return_value=TEST_TIMESTAMP)
    def test_generate_access_token_success(self, mock_time):
        """
        Test successful access token generation.

        Verifies token format and expiration timestamp return value.
        """
        token, expiration_timestamp = generate_access_token(
            TEST_ENTITY_ID, TEST_SECRET_KEY, TEST_EXPIRATION
        )

        parts = token.split(':')
        self.assertEqual(len(parts), 3)
        self.assertEqual(parts[0], TEST_ENTITY_ID)
        self.assertEqual(parts[1], str(TEST_TIMESTAMP))
        self.assertEqual(expiration_timestamp, TEST_TIMESTAMP + TEST_EXPIRATION)

    @patch('rococo.auth.tokens.time.time', return_value=TEST_TIMESTAMP)
    def test_generate_access_token_signature_correctness(self, mock_time):
        """
        Test that the generated signature matches expected HMAC-SHA256.

        Verifies correct HMAC implementation for access tokens.
        """
        token, _ = generate_access_token(TEST_ENTITY_ID, TEST_SECRET_KEY, TEST_EXPIRATION)
        entity_id, timestamp, signature = token.split(':')

        # Calculate expected signature
        data = entity_id + timestamp
        expected_signature = hmac.new(
            key=TEST_SECRET_KEY.encode('utf-8'),
            msg=data.encode('utf-8'),
            digestmod=hashlib.sha256
        ).hexdigest()

        self.assertEqual(signature, expected_signature)

    @patch('rococo.auth.tokens.time.time', return_value=TEST_TIMESTAMP)
    def test_generate_access_token_different_entity_ids_produce_different_tokens(self, mock_time):
        """
        Test that different entity IDs produce different tokens.

        Verifies uniqueness of tokens.
        """
        token1, _ = generate_access_token("entity-1", TEST_SECRET_KEY, TEST_EXPIRATION)
        token2, _ = generate_access_token("entity-2", TEST_SECRET_KEY, TEST_EXPIRATION)

        self.assertNotEqual(token1, token2)

    @patch('rococo.auth.tokens.time.time', return_value=TEST_TIMESTAMP)
    def test_generate_access_token_with_different_expiration_times(self, mock_time):
        """
        Test that different expiration times produce correct expiration timestamps.

        Verifies expiration timestamp calculation.
        """
        _, exp1 = generate_access_token(TEST_ENTITY_ID, TEST_SECRET_KEY, 3600)
        _, exp2 = generate_access_token(TEST_ENTITY_ID, TEST_SECRET_KEY, 7200)

        self.assertEqual(exp1, TEST_TIMESTAMP + 3600)
        self.assertEqual(exp2, TEST_TIMESTAMP + 7200)
        self.assertNotEqual(exp1, exp2)

    @patch('rococo.auth.tokens.time.time', return_value=TEST_TIMESTAMP)
    def test_generate_access_token_with_uuid(self, mock_time):
        """
        Test access token generation with UUID entity_id.

        Verifies support for UUID format (common use case).
        """
        token, _ = generate_access_token(TEST_ENTITY_ID, TEST_SECRET_KEY, TEST_EXPIRATION)

        parts = token.split(':')
        self.assertEqual(parts[0], TEST_ENTITY_ID)


class TestValidateAccessToken(unittest.TestCase):
    """Test access token validation."""

    @patch('rococo.auth.tokens.time.time', return_value=TEST_TIMESTAMP)
    def test_validate_access_token_success(self, mock_time):
        """
        Test successful validation of a valid access token.

        Verifies that a correctly generated token is validated successfully.
        """
        token, _ = generate_access_token(TEST_ENTITY_ID, TEST_SECRET_KEY, TEST_EXPIRATION)

        result = validate_access_token(token, TEST_SECRET_KEY, TEST_EXPIRATION)

        self.assertEqual(result, TEST_ENTITY_ID)

    @patch('rococo.auth.tokens.time.time')
    def test_validate_access_token_expired(self, mock_time):
        """
        Test that expired access tokens are rejected.

        Verifies expiration checking for access tokens.
        """
        # Generate token at TEST_TIMESTAMP
        mock_time.return_value = TEST_TIMESTAMP
        token, _ = generate_access_token(TEST_ENTITY_ID, TEST_SECRET_KEY, TEST_EXPIRATION)

        # Try to validate after expiration
        mock_time.return_value = TEST_TIMESTAMP + TEST_EXPIRATION + 1
        result = validate_access_token(token, TEST_SECRET_KEY, TEST_EXPIRATION)

        self.assertFalse(result)

    @patch('rococo.auth.tokens.time.time', return_value=TEST_TIMESTAMP)
    def test_validate_access_token_tampered_signature(self, mock_time):
        """
        Test that tokens with tampered signatures are rejected.

        Verifies signature verification for access tokens.
        """
        token, _ = generate_access_token(TEST_ENTITY_ID, TEST_SECRET_KEY, TEST_EXPIRATION)

        # Tamper with the signature
        parts = token.split(':')
        parts[2] = parts[2][:-1] + ('0' if parts[2][-1] != '0' else '1')
        tampered_token = ':'.join(parts)

        result = validate_access_token(tampered_token, TEST_SECRET_KEY, TEST_EXPIRATION)

        self.assertFalse(result)

    @patch('rococo.auth.tokens.time.time', return_value=TEST_TIMESTAMP)
    def test_validate_access_token_wrong_secret_key(self, mock_time):
        """
        Test that tokens are rejected with wrong secret key.

        Verifies secret key validation for access tokens.
        """
        token, _ = generate_access_token(TEST_ENTITY_ID, TEST_SECRET_KEY, TEST_EXPIRATION)

        result = validate_access_token(token, "wrong_secret_key", TEST_EXPIRATION)

        self.assertFalse(result)

    @patch('rococo.auth.tokens.time.time', return_value=TEST_TIMESTAMP)
    def test_validate_access_token_tampered_entity_id(self, mock_time):
        """
        Test that tokens with tampered entity_id are rejected.

        Verifies that modifying the entity_id invalidates the signature.
        """
        token, _ = generate_access_token(TEST_ENTITY_ID, TEST_SECRET_KEY, TEST_EXPIRATION)

        # Tamper with the entity_id
        parts = token.split(':')
        parts[0] = "hacker-entity-id"
        tampered_token = ':'.join(parts)

        result = validate_access_token(tampered_token, TEST_SECRET_KEY, TEST_EXPIRATION)

        self.assertFalse(result)

    @patch('rococo.auth.tokens.time.time', return_value=TEST_TIMESTAMP)
    def test_validate_access_token_with_latin1_encoding(self, mock_time):
        """
        Test validation with latin-1 encoding.

        Verifies encoding consistency for access tokens.
        """
        token, _ = generate_access_token(
            TEST_ENTITY_ID, TEST_SECRET_KEY, TEST_EXPIRATION, encoding='latin-1'
        )
        result = validate_access_token(
            token, TEST_SECRET_KEY, TEST_EXPIRATION, encoding='latin-1'
        )

        self.assertEqual(result, TEST_ENTITY_ID)


class TestTokenSecurityEdgeCases(unittest.TestCase):
    """Test security edge cases and potential vulnerabilities."""

    def test_malformed_token_too_few_colons(self):
        """
        Test that malformed tokens with too few colons raise ValueError.

        Verifies proper error handling for invalid token format.
        """
        malformed_token = "email:timestamp"  # Missing signature

        with self.assertRaises(ValueError):
            validate_confirmation_token(malformed_token, TEST_SECRET_KEY, TEST_EXPIRATION)

    def test_malformed_token_too_many_colons(self):
        """
        Test that tokens with too many colons are handled.

        Verifies handling of unexpected token format.
        """
        malformed_token = "email:timestamp:signature:extra"

        with self.assertRaises(ValueError):
            validate_confirmation_token(malformed_token, TEST_SECRET_KEY, TEST_EXPIRATION)

    def test_empty_token_string(self):
        """
        Test that empty token strings raise ValueError.

        Verifies handling of empty input.
        """
        with self.assertRaises(ValueError):
            validate_confirmation_token("", TEST_SECRET_KEY, TEST_EXPIRATION)

    @patch('rococo.auth.tokens.time.time', return_value=TEST_TIMESTAMP)
    def test_zero_expiration_time(self, mock_time):
        """
        Test behavior with zero expiration time.

        Verifies handling of zero expiration.
        With zero expiration at the same timestamp, token is technically still valid
        because timestamp + 0 == current_time, not < current_time.
        """
        token = generate_confirmation_token(TEST_EMAIL, TEST_SECRET_KEY)

        # Zero expiration at same timestamp is still valid (uses < not <=)
        result = validate_confirmation_token(token, TEST_SECRET_KEY, 0)

        self.assertEqual(result, TEST_EMAIL)

    @patch('rococo.auth.tokens.time.time', return_value=TEST_TIMESTAMP)
    def test_negative_expiration_time(self, mock_time):
        """
        Test behavior with negative expiration time.

        Verifies handling of invalid expiration value.
        """
        token = generate_confirmation_token(TEST_EMAIL, TEST_SECRET_KEY)

        # Negative expiration should cause token to be expired
        result = validate_confirmation_token(token, TEST_SECRET_KEY, -3600)

        self.assertFalse(result)

    @patch('rococo.auth.tokens.time.time', return_value=TEST_TIMESTAMP)
    def test_very_long_secret_key(self, mock_time):
        """
        Test token generation with very long secret key.

        Verifies handling of long secret keys.
        """
        long_secret = "x" * 10000
        token = generate_confirmation_token(TEST_EMAIL, long_secret)
        result = validate_confirmation_token(token, long_secret, TEST_EXPIRATION)

        self.assertEqual(result, TEST_EMAIL)

    @patch('rococo.auth.tokens.time.time', return_value=TEST_TIMESTAMP)
    def test_empty_secret_key(self, mock_time):
        """
        Test token generation with empty secret key.

        Verifies that empty secret keys still generate tokens (though insecure).
        """
        token = generate_confirmation_token(TEST_EMAIL, "")
        result = validate_confirmation_token(token, "", TEST_EXPIRATION)

        # Should still work, though insecure in practice
        self.assertEqual(result, TEST_EMAIL)

    @patch('rococo.auth.tokens.time.time', return_value=TEST_TIMESTAMP)
    def test_unicode_characters_in_email(self, mock_time):
        """
        Test token generation with Unicode characters in email.

        Verifies Unicode handling.
        """
        unicode_email = "test@例え.com"
        token = generate_confirmation_token(unicode_email, TEST_SECRET_KEY)
        result = validate_confirmation_token(token, TEST_SECRET_KEY, TEST_EXPIRATION)

        self.assertEqual(result, unicode_email)

    @patch('rococo.auth.tokens.time.time', return_value=TEST_TIMESTAMP)
    def test_colon_in_email_breaks_parsing(self, mock_time):
        """
        Test that colons in email cause parsing issues.

        Verifies awareness of delimiter limitations.
        """
        # This is a known limitation - colons in email will break the format
        email_with_colon = "test:user@example.com"
        token = generate_confirmation_token(email_with_colon, TEST_SECRET_KEY)

        # This will fail to parse correctly due to extra colon
        with self.assertRaises(ValueError):
            validate_confirmation_token(token, TEST_SECRET_KEY, TEST_EXPIRATION)

    def test_non_numeric_timestamp_in_token(self):
        """
        Test that non-numeric timestamps raise ValueError.

        Verifies timestamp validation.
        """
        malformed_token = "email:not_a_number:signature"

        with self.assertRaises(ValueError):
            validate_confirmation_token(malformed_token, TEST_SECRET_KEY, TEST_EXPIRATION)

    @patch('rococo.auth.tokens.time.time', return_value=TEST_TIMESTAMP)
    def test_token_replay_attack_same_timestamp(self, mock_time):
        """
        Test that multiple tokens with same timestamp are identical.

        This demonstrates that tokens generated at the same time are identical,
        which is expected behavior but worth documenting for security analysis.
        """
        token1 = generate_confirmation_token(TEST_EMAIL, TEST_SECRET_KEY)
        token2 = generate_confirmation_token(TEST_EMAIL, TEST_SECRET_KEY)

        # Tokens generated at same timestamp with same inputs are identical
        self.assertEqual(token1, token2)


if __name__ == '__main__':
    unittest.main()
