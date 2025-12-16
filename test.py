# Models
from dataclasses import field, dataclass
from typing import Optional
from lib_rococo.rococo.repositories.postgresql import PostgreSQLRepository
from lib_rococo.rococo.models import VersionedModel
from lib_rococo.rococo.data import PostgreSQLAdapter

@dataclass
class Email(VersionedModel):
    email_address: Optional[str] = None

@dataclass
class LoginMethod(VersionedModel):
    email_id: Optional[str] = None  # Stores the entity_id of an object of Email class.
    method_type: Optional[str] = None

@dataclass
class Person(VersionedModel):
    login_method_id: Optional[str] = None  # Stores the entity_id of an object of LoginMethod class.
    name: Optional[str] = None
    

@dataclass
class Organization(VersionedModel):
    person_id: Optional[str] = None  # Stores the entity_id of an object of Person class.
    name: Optional[str] = None


def get_db_connection():
    return PostgreSQLAdapter('localhost', 5432, 'postgres', 'password', 'bretondb1')


# **Creating and relating objects.**
email = Email(email_address="test@example.com")

# Create a LoginMethod that references Email object
login_method = LoginMethod(
    method_type="email-password",
    email_id=email.entity_id  # Reference to the Email object created previously.
)

# Create a Person that references LoginMethod object
person = Person(
    name="Axel",
    login_method_id=login_method.entity_id  # Reference to the LoginMethod object created previously.
)

# Create an Organization that references Person object
organization = Organization(
    name="Organization1",
    person_id=person.entity_id  # Reference to the Person object created previously.
)


with get_db_connection() as adapter:
    print(adapter.execute_query("""CREATE TABLE public.email (
        active boolean,
        changed_by_id text,
        entity_id text NOT NULL,
        email_address text,
        previous_version text,
        version text,
        changed_on text
    );   
    """, {}))
    # Repository usage examples have been removed - see documentation for usage patterns