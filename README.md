# rococo

A Python library to help build things the way we want them built.

_Anything worth doing is worth doing well.  Anything worth doing twice is worth doing in rococo._

## Basic Usage

### Installation

Install using pip:

```bash
pip install rococo
```

### Example

#### Models

```python
from rococo.models import Person

# Initialize a Person object from rococo's built-in models.
someone = Person(first_name="John", last_name="Doe")

# Prepare to save the object in the database adding/updating attributes for the object.
someone.prepare_for_save(changed_by_id=UUID("b30884cb-5127-457c-a633-4a800ad3c44b"))

someone.as_dict()
```

>>> OUTPUT:

```json
{
    'active': True,
    'changed_by_id': 'b30884cb-5127-457c-a633-4a800ad3c44b',
    'changed_on': datetime.datetime(2023, 9, 20, 19, 50, 23, 532875),
    'entity_id': 'ba68b3b1-fccd-4035-92f6-0ac2b29d71a1',
    'first_name': 'John',
    'last_name': 'Doe',
    'previous_version': '3261fc4d-7db4-4945-91b5-9fb6a4b7dbc5',
    'version': '4b6d92de-64bc-4dfb-a824-2151e8f11b73'
}
```

#### Messaging

##### RabbitMQ

```python
# Producer
from rococo.messaging import RabbitMqConnection

with RabbitMqConnection('host', 'port', 'username', 'password', 'virtual_host') as conn:
    conn.send_message('queue_name', {'message': 'data'})


# Consumer
from rococo.messaging import RabbitMqConnection

def process_message(message_data: dict):
    print(f"Processing message {message_data}...")

with RabbitMqConnection('host', 'port', 'username', 'password', 'virtual_host') as conn:
    conn.consume_messages('queue_name', process_message)
```

##### SQS

```python
# Producer
from rococo.messaging import SqsConnection

with SqsConnection(region_name='us-east-1') as conn:
    conn.send_message('queue_name', {'message': 'data'})


# Consumer
from rococo.messaging import SqsConnection

def process_message(message_data: dict):
    print(f"Processing message {message_data}...")

with SqsConnection(region_name='us-east-1') as conn:
    conn.consume_messages('queue_name', process_message)

# Note: since cleanup is not required for SQS connections, you can also do:
conn = SqsConnection(region_name='us-east-1')
conn.send_message('queue_name', {'message': 'data'})
conn.consume_messages('queue_name', process_message)
```

##### Processing

Processing data from messages can be achieved by implementing the abstract class `BaseServiceProcessor` within `messaging/base.py`

#### Data

##### SurrealDB

```python
from rococo.data import SurrealDbAdapter

def get_db_connection():
    endpoint = "ws://localhost:8000/rpc"
    username = "myuser"
    password = "mypassword"
    namespace = "test"
    database = "test"

    return SurrealDbAdapter(endpoint, username, password, namespace, database)


with get_db_connection() as db:
    db.execute_query("""insert into person {
        user: 'me',
        pass: 'very_safe',
        tags: ['python', 'documentation']
    };""")
    print(db.execute_query("SELECT * FROM person;", {}))
```

<summary>

##### Relationships in Surreal DB

</summary>

<details>

Consider the following example models:

```python
# Models
from dataclasses import field, dataclass
from rococo.repositories import SurrealDbRepository
from rococo.models import VersionedModel
from rococo.data import SurrealDbAdapter

@dataclass
class Email(VersionedModel):
    email_address: str = None

@dataclass
class LoginMethod(VersionedModel):
    email: str = field(default=None, metadata={
        'relationship': {'model': Email, 'type': 'direct'},
        'field_type': 'record_id'
    })
    method_type: str = None

@dataclass
class Person(VersionedModel):
    login_method: str = field(default=None, metadata={
        'relationship': {'model': LoginMethod, 'type': 'direct'},
        'field_type': 'record_id'
    })
    name: str = None
    

@dataclass
class Organization(VersionedModel):
    person: str = field(default=None, metadata={
        'relationship': {'model': Person, 'type': 'direct'},
        'field_type': 'record_id'
    })
    name: str = None


def get_db_connection():
    endpoint = "ws://localhost:8000/rpc"
    username = "root"
    password = "root"
    namespace = "breton1"
    database = "bretondb1"
    return SurrealDbAdapter(endpoint, username, password, namespace, database)





# **Creating and relating objects.**
email = Email(email_address="test@example.com")

# Create a LoginMethod that references Email object
login_method = LoginMethod(
    method_type="email-password",
    email=email  # Can be referenced by object
)

# Create a Person that references LoginMethod object
person = Person(
    name="Person1",
    login_method=login_method.entity_id  # Can be referenced by UUID object.
)

# Create an Organization that references Person object
organization = Organization(
    name="Organization1",
    person=str(person.entity_id)  # Can be referenced by UUID string.
)


with get_db_connection() as adapter:
    # **Create repositories**
    person_repo = SurrealDbRepository(adapter, Person, None, None)
    organization_repo = SurrealDbRepository(adapter, Organization, None, None)
    login_method_repo = SurrealDbRepository(adapter, LoginMethod, None, None)
    email_repo = SurrealDbRepository(adapter, Email, None, None)

    # ** Save objects.
    organization_repo.save(organization)
    # Saves to SurrealDB:
    # {
    #     "active": true,
    #     "changed_by_id": "00000000-0000-4000-8000-000000000000",
    #     "changed_on": "2023-11-23T19:21:00.816083",
    #     "id": "organization:⟨5bb0a0dc-0043-45a3-9dac-d514b5ef7669⟩",
    #     "name": "Organization1",
    #     "person": "person:⟨7a3f4e8c-fd46-43db-b619-5b2129bbcc37⟩",
    #     "previous_version": "00000000-0000-4000-8000-000000000000",
    #     "version": "b49010ad-bc64-487e-bd41-4cdf20ff7aab"
    # }

    person_repo.save(person)
    # Saves to SurrealDB:
    # {
    #     "active": true,
    #     "changed_by_id": "00000000-0000-4000-8000-000000000000",
    #     "changed_on": "2023-11-23T19:21:00.959270",
    #     "id": "person:⟨7a3f4e8c-fd46-43db-b619-5b2129bbcc37⟩",
    #     "login_method": "loginmethod:⟨0e1ef122-e4aa-435f-ad97-bd75ef6d1eb8⟩",
    #     "name": "Person1",
    #     "previous_version": "00000000-0000-4000-8000-000000000000",
    #     "version": "95049030-80bd-45cd-a39f-09139fe67343"
    # }

    login_method_repo.save(login_method)
    # Saves to SurrealDB:
    # {
    #     "active": true,
    #     "changed_by_id": "00000000-0000-4000-8000-000000000000",
    #     "changed_on": "2023-11-23T19:21:01.025179",
    #     "email": "email:⟨3e654628-47fa-4a0e-bd42-79fda124149e⟩",
    #     "id": "loginmethod:⟨0e1ef122-e4aa-435f-ad97-bd75ef6d1eb8⟩",
    #     "method_type": "email-password",
    #     "previous_version": "00000000-0000-4000-8000-000000000000",
    #     "version": "9e20a1dc-bcb1-45c2-bdf8-64e441a79758"
    # }

    email_repo.save(email)
    # Saves to SurrealDB:
    # {
    #     "active": true,
    #     "changed_by_id": "00000000-0000-4000-8000-000000000000",
    #     "changed_on": "2023-11-23T19:21:01.093089",
    #     "email_address": "test@example.com",
    #     "id": "email:⟨3e654628-47fa-4a0e-bd42-79fda124149e⟩",
    #     "previous_version": "00000000-0000-4000-8000-000000000000",
    #     "version": "0f693d94-a912-4b0a-bc96-3e558f7e13d5"
    # }


    # **Fetching related objects
    organization = organization_repo.get_one({"entity_id": organization.entity_id}, fetch_related=['person'])
    # Roughly evaluates to "SELECT * FROM organization FETCH person;"

    print(organization.person.entity_id)  # Prints entity_id of related person
    print(organization.person.name)  # Prints name of related person

    organization = organization_repo.get_one({"entity_id": organization.entity_id})
    # Roughly evaluates to "SELECT * FROM organization;"

    print(organization.person.entity_id)  # Prints entity_id of related person
    try:
        print(organization.person.name)  # raises AttributeError
    except AttributeError:
        pass

    print(organization.as_dict(True))
    # prints 
    # {
    #     "entity_id":"ff02a2c0-6bcf-426f-b5b9-8c01913b79f6",
    #     "version":"9d58fd0e-b70a-4772-91f9-af3e6342de5b",
    #     "previous_version":"00000000-0000-4000-8000-000000000000",
    #     "active": True,
    #     "changed_by_id":"00000000-0000-4000-8000-000000000000",
    #     "changed_on":"2023-11-25T12:21:09.028676",
    #     "person":{
    #         "entity_id":"93cc132c-2a2a-46e4-853a-c02239336a28"
    #     },
    #     "name":"Organization1"
    # }

    # FETCH chaining
    organization = organization_repo.get_one({"entity_id": organization.entity_id}, fetch_related=['person', 'person.login_method', 'person.login_method.email'])
    # Roughly evaluates to "SELECT * FROM organization FETCH person, person.login_method, person.login_method.email"

    print(organization.entity_id)  # Prints entity_id of organization
    print(organization.person.entity_id)  # Prints entity_id of organization.person
    print(organization.person.login_method.entity_id)  # Prints entity_id of organization.person.login_method
    print(organization.person.login_method.email.entity_id)  # Prints entity_id of organization.person.login_method.email
    print(organization.person.login_method.email.email_address)  # Prints email address of organization.person.login_method.email
    print(organization.as_dict(True))
    # prints
    # {
    #     "entity_id":"846f0756-20ab-44d3-8899-07e10b698ccd",
    #     "version":"578ca4c7-311a-4508-85ec-00ba264cd741",
    #     "previous_version":"00000000-0000-4000-8000-000000000000",
    #     "active": True,
    #     "changed_by_id":"00000000-0000-4000-8000-000000000000",
    #     "changed_on":"2023-11-25T12:19:46.541387",
    #     "person":{
    #         "entity_id":"ef99b93c-e1bb-4f37-96d5-e4e560dbdda0",
    #         "version":"b3ce7b8a-223e-4a63-a842-042178c9645c",
    #         "previous_version":"00000000-0000-4000-8000-000000000000",
    #         "active": True,
    #         "changed_by_id":"00000000-0000-4000-8000-000000000000",
    #         "changed_on":"2023-11-25T12:19:46.623192",
    #         "login_method":{
    #             "entity_id":"a7efa334-ea92-4e59-95d5-c8d51a976c1b",
    #             "version":"4d1a9a1b-81fb-433f-8b43-1bf1c3b696da",
    #             "previous_version":"00000000-0000-4000-8000-000000000000",
    #             "active": True,
    #             "changed_by_id":"00000000-0000-4000-8000-000000000000",
    #             "changed_on":"2023-11-25T12:19:46.706244",
    #             "email":{
    #                 "entity_id":"76e95956-0404-4c06-916b-89927b73d26d",
    #                 "version":"d59a91a4-26ef-4a88-bee2-b5c0d651bd77",
    #                 "previous_version":"00000000-0000-4000-8000-000000000000",
    #                 "active":True,
    #                 "changed_by_id":"00000000-0000-4000-8000-000000000000",
    #                 "changed_on":"2023-11-25T12:19:46.775098",
    #                 "email_address":"test@example.com"
    #             },
    #             "method_type":"email-password"
    #         },
    #         "name":"Person1"
    #     },
    #     "name":"Organization1"
    # }
```

###### Many-to-many relationships

```python
# Many-to-Many relationships

# **Creating and relating objects.**
# Many-to-Many relationships
# Investor->investswith->Investment
#         /\     /\    /\
#         ||     ||    ||
#         IN    name   OUT

from typing import List

@dataclass
class Investor(VersionedModel):
    name: str = None
    # One-to-Many field
    person: str = field(default=None, metadata={
        'relationship': {'model': Person, 'type': 'direct'},
        'field_type': 'record_id'
    })
    # Many-to-Many field
    investments: List[VersionedModel] = field(default=None, metadata={
        'relationship': {'model': 'Investment', 'type': 'associative', 'name': 'investswith', 'direction': 'out'},
        'field_type': 'm2m_list'
    })

@dataclass
class Investment(VersionedModel):
    name: str = None
    # Many-to-Many field
    investors: List[VersionedModel] = field(default=None, metadata={
        'relationship': {'model': 'Investor', 'type': 'associative', 'name': 'investswith', 'direction': 'in'},
        'field_type': 'm2m_list'
    })


# **Creating and relating objects.**
investor1 = Investor(name="Investor1", person=person)
investor2 = Investor(name="Investor2", person=person.entity_id)
investor3 = Investor(name="Investor3", person=Person(entity_id=person.entity_id))
investment1 = Investment(name="Investment1")
investment2 = Investment(name="Investment2")
investment3 = Investment(name="Investment3")

with get_db_connection() as adapter:
    # **Create repositories**
    investor_repo = SurrealDbRepository(adapter, Investor, None, None)
    investment_repo = SurrealDbRepository(adapter, Investment, None, None)

    investor_repo.save(investor1)
    investor_repo.save(investor2)
    investor_repo.save(investor3)
    investment_repo.save(investment1)
    investment_repo.save(investment2)
    investment_repo.save(investment3)

    # Relate investor1 to investment2 and investment3
    investor_repo.relate(investor1, 'investswith', investment2)
    # OR
    investment_repo.relate(investor1, 'investswith', Investment(entity_id=investment3.entity_id))

    # Relate investor2 to investment1 and investment3
    investor_repo.relate(Investor(entity_id=investor2.entity_id), 'investswith', investment1)
    investor_repo.relate(investor2, 'investswith', investment3)

    # Relate investor3 to investment1 and investment2
    investment_repo.relate(investor3, 'investswith', investment1)
    investment_repo.relate(investor3, 'investswith', investment2)


    # Fetching many-to-many relations
    for investment in investment_repo.get_many({}, fetch_related=['investors']):
        print("Investment: ", investment.as_dict(True))
        print()

    # Fetch-chaining for many-to-many relations
    for investment in investment_repo.get_many({}, fetch_related=['investors', 'investors.person', 'investors.person.login_method', 'investors.person.login_method.email']):
        print("Investment: ", investment.as_dict(True))
        print()

    # Get investments of an investor by investor's entity_id
    investor_with_investments = investor_repo.get_one({'entity_id': investor1.entity_id}, fetch_related=['investments'])
    investments = investor_with_investments.investments
    for investment in investments:
        print(investment.as_dict())

```

</details>

<summary>

##### Relationships in MySQL

</summary>

<details>

Consider the following example models:

```python
# Models
from dataclasses import field, dataclass
from rococo.repositories.mysql import MySqlRepository
from rococo.models import VersionedModel
from rococo.data import MySqlAdapter

@dataclass
class Email(VersionedModel):
    email_address: str = None

@dataclass
class LoginMethod(VersionedModel):
    email: str = field(default=None, metadata={
        'relationship': {'model': Email},
        'field_type': 'entity_id'
    })
    method_type: str = None

@dataclass
class Person(VersionedModel):
    login_method: str = field(default=None, metadata={
        'relationship': {'model': LoginMethod},
        'field_type': 'entity_id'
    })
    name: str = None
    

@dataclass
class Organization(VersionedModel):
    person: str = field(default=None, metadata={
        'relationship': {'model': Person},
        'field_type': 'entity_id'
    })
    name: str = None


def get_db_connection():
    return MySqlAdapter('localhost', 3306, 'root', 'ransomsnare_root_pass', 'testdb')


# **Creating and relating objects.**
email = Email(email_address="test@example.com")

# Create a LoginMethod that references Email object
login_method = LoginMethod(
    method_type="email-password",
    email=email  # Can be referenced by object
)

# Create a Person that references LoginMethod object
person = Person(
    name="Person1",
    login_method=login_method.entity_id  # Can be referenced by UUID object.
)

# Create an Organization that references Person object
organization = Organization(
    name="Organization1",
    person=str(person.entity_id)  # Can be referenced by UUID string.
)


with get_db_connection() as adapter:
    # **Create repositories**
    person_repo = MySqlRepository(adapter, Person, None, None)
    organization_repo = MySqlRepository(adapter, Organization, None, None)
    login_method_repo = MySqlRepository(adapter, LoginMethod, None, None)
    email_repo = MySqlRepository(adapter, Email, None, None)

    # ** Save objects.
    organization_repo.save(organization)
    # Saves to MySQL:
    # {
    #     "active": true,
    #     "changed_by_id": "00000000000040008000000000000000",
    #     "changed_on": "2024-03-11 00:03:21",
    #     "entity_id": "5bb0a0dc004345a39dacd514b5ef7669",
    #     "name": "Organization1",
    #     "person": "7a3f4e8cfd4643dbb6195b2129bbcc37",
    #     "previous_version": "00000000000040008000000000000000",
    #     "version": "b49010adbc64487ebd414cdf20ff7aab"
    # }

    person_repo.save(person)
    # Saves to MySQL:
    # {
    #     "active": true,
    #     "changed_by_id": "00000000000040008000000000000000",
    #     "changed_on": "2024-03-11 00:03:21",
    #     "id": "7a3f4e8cfd4643dbb6195b2129bbcc37",
    #     "login_method": "0e1ef122e4aa435fad97bd75ef6d1eb8",
    #     "name": "Person1",
    #     "previous_version": "00000000000040008000000000000000",
    #     "version": "9504903080bd45cda39f09139fe67343"
    # }

    login_method_repo.save(login_method)
    # Saves to MySQL:
    # {
    #     "active": true,
    #     "changed_by_id": "00000000000040008000000000000000",
    #     "changed_on": "2024-03-11 00:03:21",
    #     "email": "3e65462847fa4a0ebd4279fda124149e",
    #     "id": "0e1ef122e4aa435fad97bd75ef6d1eb8",
    #     "method_type": "email-password",
    #     "previous_version": "00000000000040008000000000000000",
    #     "version": "9e20a1dcbcb145c2bdf864e441a79758"
    # }

    email_repo.save(email)
    # Saves to MySQL:
    # {
    #     "active": true,
    #     "changed_by_id": "00000000000040008000000000000000",
    #     "changed_on": "2024-03-11 00:03:21",
    #     "email_address": "test@example.com",
    #     "id": "3e65462847fa4a0ebd4279fda124149e",
    #     "previous_version": "00000000000040008000000000000000",
    #     "version": "0f693d94a9124b0abc963e558f7e13d5"
    # }


    # **Fetching related objects
    organization = organization_repo.get_one({"entity_id": organization.entity_id}, join_fields=['person'])
    # Roughly evaluates to "SELECT * FROM organization INNER JOIN person ON organization.person=person.entity_id WHERE organization.entity_id=<Specified entity ID>;"

    print(organization.person.entity_id)  # Prints entity_id of related person
    print(organization.person.name)  # Prints name of related person

    organization = organization_repo.get_one({"entity_id": organization.entity_id})
    # Roughly evaluates to "SELECT * FROM organization WHERE entity_id=<Specified entity ID>;"

    print(organization.person.entity_id)  # Prints entity_id of related person
    try:
        print(organization.person.name)  # raises AttributeError
    except AttributeError:
        pass

    print(organization.as_dict(True))
    # prints 
    # {
    #     "entity_id":"fb5a9d0e-4bac-467f-9318-4063811e51b6",
    #     "version":"6fb045ef-1428-4a0c-b5a6-37c18e6711ab",
    #     "previous_version":"00000000-0000-4000-8000-000000000000",
    #     "active":1,
    #     "changed_by_id":"00000000-0000-4000-8000-000000000000",
    #     "changed_on":"2024-03-11T00:03:21",
    #     "person":{
    #         "entity_id":"582ecaad-e30f-40bc-8e6c-c4675a4bc178"
    #     },
    #     "name":"Organization1"
    # }

    # JOIN chaining
    organization = organization_repo.get_one({"entity_id": organization.entity_id}, join_fields=['person', 'person.login_method', 'person.login_method.email'])
    # Roughly evaluates to:
    # SELECT * FROM organization 
    #   INNER JOIN person ON organization.person=person.entity_id 
    #   INNER JOIN login_method ON person.login_method=login_method.entity_id 
    #   INNER JOIN email ON lgin_method.email=email.entity_id
    # WHERE organization.entity_id=<Specified entity ID>;

    print(organization.entity_id)  # Prints entity_id of organization
    print(organization.person.entity_id)  # Prints entity_id of organization.person
    print(organization.person.login_method.entity_id)  # Prints entity_id of organization.person.login_method
    print(organization.person.login_method.email.entity_id)  # Prints entity_id of organization.person.login_method.email
    print(organization.person.login_method.email.email_address)  # Prints email address of organization.person.login_method.email
    print(organization.as_dict(True))
    # prints
    # {
    #     "entity_id":"846f0756-20ab-44d3-8899-07e10b698ccd",
    #     "version":"578ca4c7-311a-4508-85ec-00ba264cd741",
    #     "previous_version":"00000000-0000-4000-8000-000000000000",
    #     "active": True,
    #     "changed_by_id":"00000000-0000-4000-8000-000000000000",
    #     "changed_on":"2023-11-25T12:19:46.541387",
    #     "person":{
    #         "entity_id":"ef99b93c-e1bb-4f37-96d5-e4e560dbdda0",
    #         "version":"b3ce7b8a-223e-4a63-a842-042178c9645c",
    #         "previous_version":"00000000-0000-4000-8000-000000000000",
    #         "active": True,
    #         "changed_by_id":"00000000-0000-4000-8000-000000000000",
    #         "changed_on":"2023-11-25T12:19:46.623192",
    #         "login_method":{
    #             "entity_id":"a7efa334-ea92-4e59-95d5-c8d51a976c1b",
    #             "version":"4d1a9a1b-81fb-433f-8b43-1bf1c3b696da",
    #             "previous_version":"00000000-0000-4000-8000-000000000000",
    #             "active": True,
    #             "changed_by_id":"00000000-0000-4000-8000-000000000000",
    #             "changed_on":"2023-11-25T12:19:46.706244",
    #             "email":{
    #                 "entity_id":"76e95956-0404-4c06-916b-89927b73d26d",
    #                 "version":"d59a91a4-26ef-4a88-bee2-b5c0d651bd77",
    #                 "previous_version":"00000000-0000-4000-8000-000000000000",
    #                 "active":True,
    #                 "changed_by_id":"00000000-0000-4000-8000-000000000000",
    #                 "changed_on":"2023-11-25T12:19:46.775098",
    #                 "email_address":"test@example.com"
    #             },
    #             "method_type":"email-password"
    #         },
    #         "name":"Person1"
    #     },
    #     "name":"Organization1"
    # }

    # Get all organizations by person
    person_orgs = organization_repo.get_many({
        "person": person.entity_id
    })
    for org in person_orgs:
        print(org.as_dict(True))
    # Prints:
    # {
    #     "entity_id":"0af9964d-0fc7-4128-ba7f-a66a51a87231",
    #     "version":"ce694166-5ca6-43dc-936d-078011469465",
    #     "previous_version":"00000000-0000-4000-8000-000000000000",
    #     "active":1,
    #     "changed_by_id":"00000000-0000-4000-8000-000000000000",
    #     "changed_on":"2024-03-11T00:14:07",
    #     "person":{
    #         "entity_id":"5b10a75a-23d7-4b98-b35e-0f1a59ec5b6d"
    #     },
    #     "name":"Organization1"
    # }

```

</details>


### How to use the adapter and base Repository in another projects

```python
class LoginMethodRepository(BaseRepository):
    def __init__(self, adapter, message_adapter, queue_name):
        super().__init__(adapter, LoginMethod, message_adapter, queue_name)

    def save(self, login_method: LoginMethod, send_message: bool = False):
        with self.adapter:
            return super().save(login_method,send_message)

    def get_one(self, conditions: Dict[str, Any]):
        with self.adapter:
            return super().get_one(conditions)

    def get_many(self, conditions: Dict[str, Any]):
        with self.adapter:
            return super().get_many(conditions)

```

- The LoginMethodRepository class is a concrete implementation of the BaseRepository class. It is responsible for managing LoginMethod objects in the database.

    The __init__() method takes an adapter object as input. This adapter object is responsible for communicating with the database. The adapter object is passed to the super().__init__() method, which initializes the base repository class.
    It also takes in a message adapter and queue name for RabbitMQ and SQS messaging which can later be used in the save() method by passing a boolean.

    The save() method takes a LoginMethod object as input and saves it to the database. The get_one() method takes a dictionary of conditions as input and returns a single LoginMethod object that matches those conditions. The get_many() method takes a dictionary of conditions as input and returns a list of LoginMethod objects that match those conditions.

#### RepositoryFactory

```python
class RepositoryFactory:
    _repositories = {}

    @classmethod
    def _get_db_connection(cls):
        endpoint = "ws://localhost:8000/rpc"
        username = "myuser"
        password = "mypassword"
        namespace = "hell"
        db_name = "abclolo"
        return SurrealDbAdapter(endpoint, username, password, namespace, db_name)

    @classmethod
    def get_repository(cls, repo_class: Type[BaseRepository]):
        if repo_class not in cls._repositories:
            adapter = cls._get_db_connection()
            cls._repositories[repo_class] = repo_class(adapter)
        return cls._repositories[repo_class]

```

- The RepositoryFactory class is a singleton class that is responsible for creating and managing repositories. It uses a cache to store the repositories that it has already created. This allows it to avoid creating the same repository multiple times.

    The _get_db_connection() method creates a new database connection using the specified endpoint, username, password, namespace, and database name. The get_repository() method takes a repository class as input and returns the corresponding repository object. If the repository object does not already exist in the cache, then the factory will create a new one and add it to the cache.

#### Sample usage

```python
sample_data = LoginMethod(
    person_id="asd123123",
    method_type="email",
    method_data={},
    email="user@example.com",
    password="hashed_password",
)

repo = RepositoryFactory.get_repository(LoginMethodRepository)

result = repo.save(sample_data)

print("Done", repo.get_one({}))
```

- The above code creates a new LoginMethod object and saves it to the database using the LoginMethodRepository object. It then retrieves the saved object from the database and prints it to the console.

    This is just a simple example of how to use the LoginMethodRepository and RepositoryFactory classes. You can use these classes to manage any type of object in a database.

### Deployment

The process described is a Continuous Integration (CI) and Continuous Deployment (CD) pipeline for a Python package using _GitHub Actions_. Here's the breakdown:

### Development Phase

Developers push their changes directly to the main branch.
This branch is likely used for ongoing development work.

### Staging/Testing Phase

When the team is ready to test a potential release, they push the code to a staging branch.
Once the code is pushed to this branch, _GitHub Actions_ automatically publishes the package to the test PyPi server.
The package can then be reviewed and tested by visiting <https://test.pypi.org/project/rococo/>.
This step ensures that the package works as expected on the PyPi platform without affecting the live package.

### Release/Publish Phase

When the team is satisfied with the testing and wants to release the package to the public, they create and publish a release on the GitHub repository.
Following this action, _GitHub Actions_ takes over and automatically publishes the package to the official PyPi server.
The package can then be accessed and downloaded by the public at <https://pypi.org/project/rococo/>.

In essence, there are three primary phases:

1. Development (main branch)
2. Testing (staging branch with test PyPi server)
3. Release (triggered by a GitHub release and published to the official PyPi server).
