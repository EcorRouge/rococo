"""
Models for rococo
"""

from .versioned_model import BaseModel, VersionedModel

# Backward compatibility alias - NonVersionedModel is now just BaseModel
NonVersionedModel = BaseModel

from .login_method import LoginMethod
from .organization import Organization
from .recovery_code import RecoveryCode
from .otp_method import OtpMethod
from .person import Person
from .person_organization_role import PersonOrganizationRole
from .email import Email
