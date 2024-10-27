def get_template(new_version, current_db_version):
    return f"""revision = "{new_version}"
down_revision = "{current_db_version}"



def upgrade(migration):
    # write migration here

    migration.update_version_table(version=revision)


def downgrade(migration):
    # write migration here

    migration.update_version_table(version=down_revision)

"""
