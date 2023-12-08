import copy
from typing import List, Union
from .surreal_db_repository import SurrealDbRepository
from rococo.models import PersonOrganizationRole


class PersonOrganizationRoleRepository(SurrealDbRepository):
    def __init__(self, adapter, message_adapter, message_queue_name):
        super().__init__(adapter, PersonOrganizationRole, message_adapter, message_queue_name)

    def find_organizations_by_member(self, member_id: str, get_member: bool = False) -> List[PersonOrganizationRole]:
        """finds all the organizations that a Person identified by member_id is a member of 

        Args:
            member_id (str): `person.entity_id`
            get_member (bool, optional): should a member (Person) be fetched too?. Defaults to False.

        Returns:
            List[PersonOrganizationRole]: found PersonOrganizationRole
        """
        conditions = {"person.entity_id": member_id}
        fetch_related: List[str] = ["organization", "person"] if get_member else ["organization"]
        return self.get_many(conditions, fetch_related=fetch_related)
    
    def find_by_owner(self, owner_id: str, get_owner: bool = False) -> List[PersonOrganizationRole]:
        # TODO
        conditions = {"person.entity_id": owner_id}
        fetch_related: List[str] = ["organization", "person"] if get_owner else ["organization"]
        return self.get_many(conditions, fetch_related=fetch_related)

    # in the case we chose to have `role` as a `PersonOrganizationRoleEnum` instead of `str`
    #  def save(self, instance: PersonOrganizationRole, send_message: bool = False) -> PersonOrganizationRole:
    #     instance_copy = copy.copy(instance)
    #     instance_copy.role = instance.role.value
    #     super().save(instance=instance_copy, send_message=send_message)
