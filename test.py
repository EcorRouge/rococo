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
    # **Create repositories**
    # person_repo = PostgreSQLRepository(adapter, Person, None, None)
    # organization_repo = PostgreSQLRepository(adapter, Organization, None, None)
    # login_method_repo = PostgreSQLRepository(adapter, LoginMethod, None, None)
    # email_repo = PostgreSQLRepository(adapter, Email, None, None)

    # # ** Save objects.
    # organization_repo.save(organization)
    # # Saves to MySQL:
    # # {
    # #     "active": true,
    # #     "changed_by_id": "00000000000040008000000000000000",
    # #     "changed_on": "2024-03-11 00:03:21",
    # #     "entity_id": "5bb0a0dc004345a39dacd514b5ef7669",
    # #     "name": "Organization1",
    # #     "person_id": "7a3f4e8cfd4643dbb6195b2129bbcc37",
    # #     "previous_version": "00000000000040008000000000000000",
    # #     "version": "b49010adbc64487ebd414cdf20ff7aab"
    # # }

    # person_repo.save(person)
    # # Saves to MySQL:
    # # {
    # #     "active": true,
    # #     "changed_by_id": "00000000000040008000000000000000",
    # #     "changed_on": "2024-03-11 00:03:21",
    # #     "id": "7a3f4e8cfd4643dbb6195b2129bbcc37",
    # #     "login_method_id": "0e1ef122e4aa435fad97bd75ef6d1eb8",
    # #     "name": "Axel",
    # #     "previous_version": "00000000000040008000000000000000",
    # #     "version": "9504903080bd45cda39f09139fe67343"
    # # }

    # login_method_repo.save(login_method)
    # # Saves to MySQL:
    # # {
    # #     "active": true,
    # #     "changed_by_id": "00000000000040008000000000000000",
    # #     "changed_on": "2024-03-11 00:03:21",
    # #     "email_id": "3e65462847fa4a0ebd4279fda124149e",
    # #     "id": "0e1ef122e4aa435fad97bd75ef6d1eb8",
    # #     "method_type": "email-password",
    # #     "previous_version": "00000000000040008000000000000000",
    # #     "version": "9e20a1dcbcb145c2bdf864e441a79758"
    #     # }

    # email_repo.save(email)
    # # Saves to MySQL:
    # # {
    # #     "active": true,
    # #     "changed_by_id": "00000000000040008000000000000000",
    # #     "changed_on": "2024-03-11 00:03:21",
    # #     "email_address": "test@example.com",
    # #     "id": "3e65462847fa4a0ebd4279fda124149e",
    # #     "previous_version": "00000000000040008000000000000000",
    # #     "version": "0f693d94a9124b0abc963e558f7e13d5"
    # # }


    # # **Fetching related objects
    # organization = organization_repo.get_one({"entity_id": organization.entity_id})
    # # Roughly evaluates to "SELECT * FROM organization WHERE entity_id=<Specified entity ID> LIMIT 1;"

    # print(organization.person_id)  # Prints entity_id of related person
    # print(organization.as_dict(True))

    # # prints 
    # # {
    # #     "entity_id":"fb5a9d0e-4bac-467f-9318-4063811e51b6",
    # #     "version":"6fb045ef-1428-4a0c-b5a6-37c18e6711ab",
    # #     "previous_version":"00000000-0000-4000-8000-000000000000",
    # #     "active":1,
    # #     "changed_by_id":"00000000-0000-4000-8000-000000000000",
    # #     "changed_on":"2024-03-11T00:03:21",
    # #     "person_id": "582ecaade30f40bc8e6cc4675a4bc178",
    # #     "name":"Organization1"
    # # }
    
    # person = person_repo.get_one({"entity_id": organization.person_id})

    # # Get all organizations by person
    # person_orgs = organization_repo.get_many({
    #     "person_id": person.entity_id
    # })
    # for org in person_orgs:
    #     print(org.as_dict(True))
    # # Prints:
    # # {
    # #     "entity_id":"0af9964d-0fc7-4128-ba7f-a66a51a87231",
    # #     "version":"ce694166-5ca6-43dc-936d-078011469465",
    # #     "previous_version":"00000000-0000-4000-8000-000000000000",
    # #     "active":1,
    # #     "changed_by_id":"00000000-0000-4000-8000-000000000000",
    # #     "changed_on":"2024-03-11T00:14:07",
    # #     "person_id": "5b10a75a-23d7-4b98-b35e-0f1a59ec5b6d",
    # #     "name":"Organization1"
    # # }