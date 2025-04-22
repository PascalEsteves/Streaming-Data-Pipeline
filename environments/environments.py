import os
from dotenv import load_dotenv

load_dotenv()

class Environments:

    @classmethod
    def get_api_url(self):
        return os.environ.get("API_URL")
    