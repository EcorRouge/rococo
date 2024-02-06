"""
OtpMethod model
"""

from dataclasses import dataclass
from typing import List

from rococo.models.mysql import VersionedModel


@dataclass
class OtpMethod(VersionedModel):
    """An OTP method model."""

    person_id: str = None
    secret: str = None
    name: str = None
    enabled: bool = False
    recovery_codes: List[str] = None
