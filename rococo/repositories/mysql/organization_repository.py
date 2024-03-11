from rococo.models import Organization
from rococo.repositories import MySqlRepository


class OrganizationRepository(MySqlRepository):
    def __init__(self, adapter, message_adapter, message_queue_name):
        super().__init__(adapter, Organization, message_adapter, message_queue_name)

    def find_by_name(self, name: str):
        conditions = {"name": name}
        return self.get_one(conditions)

    def update_organization_name(self, organization_id: str, new_name: str):
        instance = self.get_one({"entity_id": organization_id})
        if instance:
            instance.name = new_name
            return self.save(instance)
        return None
