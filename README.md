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
someone.prepare_for_save(changed_by_id="jane_doe")

someone.as_dict()

{
    'active': True,
    'changed_by_id': 'jane_doe',
    'changed_on': datetime.datetime(2023, 9, 20, 19, 50, 23, 532875),
    'entity_id': 'e06876705b364640a20efc165f6ffb76',
    'first_name': 'John',
    'last_name': 'Doe',
    'previous_version': '7e63a5d0aa0f43b5aa9c8cc0634c41f2',
    'version': '08489d2bc5d74f78b7af0f2c1d9c5498'
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
Processing data from messages can be achieved by implementing the abstract class BaseServiceProcessor within messaging/base.py 

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

The process described is a Continuous Integration (CI) and Continuous Deployment (CD) pipeline for a Python package using Github Actions. Here's the breakdown:

Development Phase:

Developers push their changes directly to the main branch.
This branch is likely used for ongoing development work.
Staging/Testing Phase:

When the team is ready to test a potential release, they push the code to a staging branch.
Once code is pushed to this branch, Github Actions automatically publishes the package to the test PyPi server.
The package can then be reviewed and tested by visiting https://test.pypi.org/project/rococo/.
This step ensures that the package works as expected on the PyPi platform without affecting the live package.
Release/Publish Phase:

When the team is satisfied with the testing and wants to release the package to the public, they create and publish a release on the Github repository.
Following this action, Github Actions takes over and automatically publishes the package to the official PyPi server.
The package can then be accessed and downloaded by the public at https://pypi.org/project/rococo/.
In essence, there are three primary phases:

Development (main branch)
Testing (staging branch with test PyPi server)
Release (triggered by a Github release and publishes to official PyPi server).
