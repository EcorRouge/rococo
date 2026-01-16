"""
OtpMethod model
"""

from dataclasses import dataclass, field
from typing import List, Optional

from . import SurrealVersionedModel


@dataclass
class OtpMethod(SurrealVersionedModel):
    """An OTP method model."""

    person: Optional[str] = field(default=None, metadata={
        'relationship': {'model': 'Person', 'type': 'direct'},
        'field_type': 'record_id'
    })
    secret: Optional[str] = None
    name: Optional[str] = None
    enabled: bool = False
    recovery_codes: Optional[List[str]] = None
