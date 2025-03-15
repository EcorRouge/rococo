

class MigrationBase:
    def __init__(self, db_adapter):
        self.db_adapter = db_adapter

    def execute(self, query, commit: bool = True, args=None):
        with self.db_adapter:
            db_response = self.db_adapter.execute_query(query, args)
            result = self.db_adapter.parse_db_response(db_response)
            if commit:
                self.db_adapter._connection.commit()
            return result
