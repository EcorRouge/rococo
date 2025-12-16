"""
OtpMethod model
"""

from dataclasses import dataclass
from typing import Optional

from . import VersionedModel


@dataclass
class OtpMethod(VersionedModel):
    """An OTP method model."""

    person_id: Optional[str] = None
    secret: Optional[str] = None
    name: Optional[str] = None
    enabled: bool = False
