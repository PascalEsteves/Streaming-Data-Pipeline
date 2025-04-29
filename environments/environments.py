import os
from dotenv import load_dotenv

load_dotenv()

class Environments:

    db_configurations = {
        "MYSQL_HOST": os.environ.get("MYSQL_HOST"),
        "MYSQL_USER": os.environ.get("MYSQL_USER"),
        "MYSQL_PASSWORD" : os.environ.get("MYSQL_PASSWORD"),
        "MYSQL_DB": os.environ.get("MYSQL_DB"),
        "MYSQL_PORT": os.environ.get("MYSQL_PORT")
    }

    @classmethod
    def get_api_url(self):
        return os.environ.get("API_URL")
    
    @classmethod
    def get_db_host(self)->str:
        return self.db_configurations.get("MYSQL_HOST")
    
    @classmethod
    def get_db_user(self)->str:
        return self.db_configurations.get("MYSQL_USER")

    @classmethod
    def get_db_password(self)->str:
        return self.db_configurations.get("MYSQL_PASSWORD")
    
    @classmethod
    def get_db_name(self)->str:
        return self.db_configurations.get("MYSQL_DB")
    
    @classmethod
    def get_db_port(self)->str:
        return self.db_configurations.get("MYSQL_PORT")