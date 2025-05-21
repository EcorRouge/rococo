# Rococo Repositories Spec

Repositories are a core part of the Rococo architecture. They provide a way to interact with the underlying data storage, I.e. database. This spec outlines how to define and work with repositories in Rococo.

- Repositories are defined in the `common/repositories` directory. 
- Each repository is a Python file that corresponds to a model file and is named as `<model_filename>.py`. 
- It defines a class inheriting from `common.repositories.base.BaseRepository`. 
- The repository class should define methods for interacting with the data storage, such as `create`, `read`, `update`, and `delete` (CRUD) operations.


The base class defines the following methods:
- `def save(self, instance: VersionedModel, send_message: bool = False) -> VersionedModel`
    Saves the instance to the database. If `send_message` is `True`, it sends a message to the message queue.
- `def delete(self, instance: VersionedModel) -> VersionedModel`
    Soft-deletes the instance from the database. Setting `active` to `False` and returns the deleted model.
- `def get_one(self, conditions: Dict[str, Any]) -> VersionedModel`
    Retrieves a single instance from the database based on the provided conditions.
- `def get_all(self, conditions: Dict[str, Any]) -> List[VersionedModel]`
    Retrieves all instances from the database based on the provided conditions.

## Repository Example


- Example 1: Barebones repository example with only methods from base class.

```python
from common.repositories.base import BaseRepository
from common.models import Organization


class OrganizationRepository(BaseRepository):
    MODEL = Organization
```


- Example 2: Repository example with helper methods. The helper methods can be used to perform any arbitrary logic on the data before saving it to the database or after retrieving it from the database.

```python
from common.repositories.base import BaseRepository
from common.models import Organization


class OrganizationRepository(BaseRepository):
    MODEL = Organization

    def get_organization_by_id(self, entity_id: str) -> Organization:
        return self.get_one({"entity_id": organization_id})

    def get_organizations_by_person_id(self, person_id: str) -> List[Organization]:
        return self.get_all({"person_id": person_id})

    def update_organization(self, organization: Organization) -> Organization:
        return self.save(organization)
    
    def create_broker_organization(self, organization: Organization) -> Organization:
        organization.organization_type = OrganizationTypeEnum.BROKER
        return self.save(organization)

    def update_organization_website(self, organization_id: str, website: str) -> Organization:
        organization = self.get_organization_by_id(organization_id)
        organization.website = website
        return self.save(organization)
```

- The OrganizationRepository class is created in `common/repositories/organization.py`. Similar format should be followed for all repositories.
- A newly added repository should be added to the `common/repositories/__init__.py` file to make it available for use in the rest of the application.
- A newly added repository should be added to the `RepoType` enum class in `common/repositories/factory.py` as follows:

```python
class RepoType(Enum):

    ...

    PERSON = auto()
    ORGANIZATION = auto()  # <-- New repository added here
```

- A newly added repository should be added to the `RepositoryFactory._repositories` dictionary in `common/repositories/factory.py` as follows:

```python
class RepositoryFactory:

    ...

    _repositories = {
        ...,
        RepoType.ORGANIZATION: OrganizationRepository,  # <-- New repository added here
    }
```


## Repository Usage

Repositories can be used in standalone services located at `services/<service_name>/lib/` or in the web application located at `flask/app/`.

The repositories are imported from `common.repositories` and used as follows:

```python
from common.repositories import RepositoryFactory, RepoType


def do_something():
    organization_repo = RepositoryFactory.get_repository(RepoType.ORGANIZATION)
    organization = organization_repo.get_organization_by_id("1234567890")
    organization.name = "New Organization Name"
    organization_repo.update_organization(organization)
```
