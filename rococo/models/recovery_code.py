"""
RecoveryCode model
"""

from dataclasses import dataclass, field
from typing import Optional

from . import VersionedModel


@dataclass
class RecoveryCode(VersionedModel):
    """An OTP method model."""

    otp_method: Optional[str] = field(default=None, metadata={
        'relationship': {'model': 'OtpMethod'},
        'field_type': 'entity_id'
    })
    secret: Optional[str] = None
    name: Optional[str] = None
    enabled: bool = False
