from abc import abstractmethod


class DbAdapter:

    @abstractmethod
    def execute_query(self, query: str):
        """Abstract method for executing a query.
        
        Args:
            query (str): The query to execute.
        """
        pass    
