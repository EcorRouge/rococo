import hashlib
import hmac
import time


def generate_confirmation_token(email, secret_key: str, encoding: str = 'utf-8'):
    """Create a token by combining email and current timestamp"""
    timestamp = str(int(time.time()))
    data = email + timestamp
    # Hash the data using HMAC algorithm with a secret key
    signature = hmac.new(
        key=secret_key.encode(encoding),
        msg=data.encode(encoding),
        digestmod=hashlib.sha256
    ).hexdigest()
    return f"{email}:{timestamp}:{signature}"


def validate_confirmation_token(token, secret_key: str, expiration: int, encoding: str = 'utf-8'):
    """Validates confirmation tokens"""
    try:
        # Split the token into email, timestamp, and signature parts
        email, timestamp, signature = token.split(':')
        # Check if the token has expired
        if int(timestamp) + expiration < int(time.time()):
            return False
        # Recreate the signature using the provided email and timestamp
        expected_signature = hmac.new(
            key=secret_key.encode(encoding),
            msg=(email + timestamp).encode(encoding),
            digestmod=hashlib.sha256
        ).hexdigest()
        # Verify if the recreated signature matches the provided one
        if hmac.compare_digest(signature, expected_signature):
            return email
        else:
            return False
    except Exception as ex:
        raise (ex)


def generate_access_token(entity_id, secret_key: str, expiration: int, encoding: str = 'utf-8'):
    """Similar to generate_confirmation_token, but using entity_id instead of email"""
    timestamp = str(int(time.time()))
    data = str(entity_id) + timestamp
    signature = hmac.new(
        key=secret_key.encode(encoding),
        msg=data.encode(encoding),
        digestmod=hashlib.sha256
    ).hexdigest()
    return f"{entity_id}:{timestamp}:{signature}", int(timestamp) + expiration


def validate_access_token(token, secret_key: str, expiration: int, encoding: str = 'utf-8'):
    """Similar to validate_confirmation_token, but for access tokens"""
    try:
        entity_id, timestamp, signature = token.split(':')
        if int(timestamp) + expiration < int(time.time()):
            return False
        expected_signature = hmac.new(
            key=secret_key.encode(encoding),
            msg=(entity_id + timestamp).encode(encoding),
            digestmod=hashlib.sha256
        ).hexdigest()
        if hmac.compare_digest(signature, expected_signature):
            return entity_id
        return False
    except Exception as ex:
        raise (ex)
