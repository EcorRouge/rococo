import copy
from typing import List, Union
from rococo.repositories.surrealdb import SurrealDbRepository
from rococo.models.surrealdb import PersonOrganizationRole


class PersonOrganizationRoleRepository(SurrealDbRepository):
    def __init__(self, adapter, message_adapter, message_queue_name):
        super().__init__(adapter, PersonOrganizationRole, message_adapter, message_queue_name)

    def find_organizations_by_member(self, member_id: str, get_member: bool = False) -> List[PersonOrganizationRole]:
        """finds all the organizations that a Person identified by member_id is a member of 

        Args:
            member_id (str): `person.id`
            get_member (bool, optional): should a member (Person) be fetched too?. Defaults to False.

        Returns:
            List[PersonOrganizationRole]: found PersonOrganizationRole
        """
        conditions = {"person": member_id}
        fetch_related: List[str] = ["organization", "person"] if get_member else ["organization"]
        return self.get_many(conditions, fetch_related=fetch_related)
    
    def find_organizations_by_owner(self, owner_id: str, get_owner: bool = False) -> List[PersonOrganizationRole]:
        """finds all the organizations that a Person identified by member_id is an owner of 

        Args:
            owner_id (str): `person.id`
            get_owner (bool, optional): should an owner (Person) be fetched too?. Defaults to False.

        Returns:
            List[PersonOrganizationRole]: found PersonOrganizationRole-s
        """
        print(f"[find_organizations_by_owner] owner_id: {owner_id}", flush=True)
        OWNER: str = "OWNER"
        conditions = {"person": owner_id, "role": OWNER}
        fetch_related: List[str] = ["organization", "person"] if get_owner else ["organization"]
        return self.get_many(conditions, fetch_related=fetch_related)