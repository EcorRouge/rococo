"""OrganizationRepo class"""
from rococo.models.surrealdb import Organization
from rococo.repositories.surrealdb import SurrealDbRepository


class OrganizationRepository(SurrealDbRepository):
    """OrganizationRepo class"""
    def __init__(self, adapter, message_adapter, message_queue_name):
        super().__init__(adapter, Organization, message_adapter, message_queue_name)

    def find_by_name(self, name: str):
        """Find organization by name"""
        conditions = {"name": name}
        return self.get_one(conditions)

    def update_organization_name(self, organization_id: str, new_name: str):
        """Update organization name"""
        instance = self.get_one({"id": organization_id})
        if instance:
            instance.name = new_name
            return self.save(instance)
        return None
