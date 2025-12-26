"""
Models for rococo
"""

from .versioned_model import BaseModel, VersionedModel

# NonVersionedModel is an alias for BaseModel (the unversioned model).
# Use either name - they are identical:
#   - BaseModel: Technical name (base of model hierarchy)
#   - NonVersionedModel: Descriptive name (emphasizes it's unversioned)
# Both refer to the same unversioned model class.
NonVersionedModel = BaseModel
from .login_method import LoginMethod
from .organization import Organization
from .recovery_code import RecoveryCode
from .otp_method import OtpMethod
from .person import Person
from .person_organization_role import PersonOrganizationRole
from .email import Email
