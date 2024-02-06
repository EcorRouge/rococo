"""
Person model
"""

from dataclasses import dataclass

from rococo.models.mysql import VersionedModel


@dataclass
class Person(VersionedModel):
    """A person model."""

    first_name: str = None
    last_name: str = None
