# rococo
A Python library to help build things the way we want them built.


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

### Implementation using Repository 

```python
class FlaskBaseRepository:
    def __init__(self):
        endpoint = "ws://localhost:8000/rpc"
        username = "myuser"
        password = "mypassword"
        namespace = "hell"
        db_name = "abclolo"
        self.adapter = SurrealDbAdapter(endpoint, username, password, namespace, db_name)

    def get_db_connection(self):
        return self.adapter


class LoginMethodRepository(BaseRepository):
    def __init__(self):
        super().__init__(FlaskBaseRepository().get_db_connection(), LoginMethod)

    def save(self, login_method: LoginMethod):
        with self.adapter:
            return super().save(login_method)

    def get_one(self, conditions: Dict[str, Any]):
        with self.adapter:
            return super().get_one(conditions)
        
    def get_many(self, conditions: Dict[str, Any]):
        with self.adapter:
            return super().get_many(conditions)

# Create a new LoginMethod instance
sample_data = LoginMethod(
    person_id="asd123123",
    method_type="email",
    method_data={},
    email="user@example.com",
    password="hashed_password"
)

# Instantiate the LoginMethodRepository
repo = LoginMethodRepository()

result = repo.save(sample_data)

print("Done",repo.get_one({}))   
print("Done",repo.get_many({}))   
```

Explanation : 
- Sure, let's go step by step:
    ### Class Definition child repository:
    ```python
    class LoginMethodRepository(BaseRepository):
    ```
    Here, you're defining a new class called LoginMethodRepository which is a child class of BaseRepository. This means it will inherit properties and methods from the BaseRepository class.

    ### Constructor Method:
    ```python
    def __init__(self):
        super().__init__(FlaskBaseRepository().get_db_connection(), LoginMethod)
    ```
    The __init__ method is the constructor. Whenever an object of the LoginMethodRepository class is created, this method will be called.

    super() is a built-in function that returns a temporary object of the superclass, which allows you to call its methods.

    super().__init__(FlaskBaseRepository().get_db_connection(), LoginMethod) is calling the constructor of the BaseRepository class. This is passing two arguments:

    A database connection obtained via the get_db_connection() method of the FlaskBaseRepository class.
    The LoginMethod class which likely represents the model for the data structure.



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