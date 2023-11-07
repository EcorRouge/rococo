"""
Person model
"""

from dataclasses import dataclass

from . import VersionedModel


@dataclass
class Person(VersionedModel):
    """A person model."""

    first_name: str
    last_name: str
