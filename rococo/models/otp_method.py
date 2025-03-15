"""
OtpMethod model
"""

from dataclasses import dataclass, field

from . import VersionedModel


@dataclass
class OtpMethod(VersionedModel):
    """An OTP method model."""

    person_id: str = None
    secret: str = None
    name: str = None
    enabled: bool = False
