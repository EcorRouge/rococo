"""
Person model
"""

from dataclasses import dataclass

from rococo.models import BaseVersionedModel


@dataclass
class BasePerson(BaseVersionedModel):
    """A person model."""

    first_name: str = None
    last_name: str = None
