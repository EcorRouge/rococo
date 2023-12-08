from rococo.models import Person
from rococo.data import SurrealDbAdapter

####################
# #### Models #######
####################

print("\n\n##### Models #######\n\n")

# Initialize a Person object from rococo's built-in models.
someone = Person(first_name="John", last_name="Doe")

print(Person.__base__.fields())

# Prepare to save the object in the database adding/updating attributes for the object.
someone.prepare_for_save(changed_by_id="jane_doe")

# someone.as_dict()

# {
#     'active': True,
#     'changed_by_id': 'jane_doe',
#     'changed_on': datetime.datetime(2023, 9, 20, 19, 50, 23, 532875),
#     'entity_id': 'e06876705b364640a20efc165f6ffb76',
#     'first_name': 'John',
#     'last_name': 'Doe',
#     'previous_version': '7e63a5d0aa0f43b5aa9c8cc0634c41f2',
#     'version': '08489d2bc5d74f78b7af0f2c1d9c5498'
# }

print("someone", someone)

someone.first_name = "SiR"

someone.prepare_for_save(changed_by_id="Rosa")

print("someone", someone)

print("\n\n##### Data / SurrealDB #######\n\n")


##############################
# ##### Data / SurrealDB ######
##############################

def get_db_connection():
    endpoint = "ws://localhost:8000/rpc"
    username = "myuser"
    password = "mypassword"
    namespace = "test"
    database = "test"

    return SurrealDbAdapter(endpoint, username, password, namespace, database)


with get_db_connection() as db:
    print("db", db)
    db.execute_query("""insert into person {
        user: 'me',
        pass: 'very_safe',
        tags: ['python', 'documentation']
    };""")
    print(db.execute_query("SELECT * FROM person;", {}))
    print("get_one('person')     ", db.get_one("person"))
    print("get_one_unconsolidated('person') ", db.get_one_unconsolidated("person", None))
