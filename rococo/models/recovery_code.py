"""
RecoveryCode model
"""

from dataclasses import dataclass, field

from . import VersionedModel


@dataclass
class RecoveryCode(VersionedModel):
    """An OTP method model."""

    otp_method: str = field(default=None, metadata={
        'relationship': {'model': 'OtpMethod'},
        'field_type': 'entity_id'
    })
    secret: str = None
    name: str = None
    enabled: bool = False
