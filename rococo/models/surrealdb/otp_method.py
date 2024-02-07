"""
OtpMethod model
"""

from dataclasses import dataclass, field
from rococo.models import BaseOtpMethod
from rococo.models.surrealdb import VersionedModel


@dataclass
class OtpMethod(VersionedModel, BaseOtpMethod):
    """An OTP method model."""

    person: str = field(default=None, metadata={
        'relationship': {'model': 'Person', 'type': 'direct'},
        'field_type': 'record_id'
    })
