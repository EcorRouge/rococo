"""
OtpMethod model
"""

from dataclasses import dataclass
from typing import List

from . import VersionedModel


@dataclass
class OtpMethod(VersionedModel):
    """An OTP method model."""

    person_id: str
    secret: str
    name: str
    enabled: bool
    recovery_codes: List[str]
