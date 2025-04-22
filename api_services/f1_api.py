import requests
from environments.environments import Environments

class F1_API:

    def __init__(self):
        self._base_url = Environments.get_api_url()

    def get_lap_time(self, year:str ,driver:str, race_id:str, lap_num:str):
        path = f"{self._base_url}/{year}/{race_id}/drivers/{driver}/laps/{lap_num}.json"
        return self.get(path= path)
    
    def get_circuits(self, year:str):
        path = f"{self._base_url}/{year}/circuits.json"
        return self.get(path=path)

    def get(self, path):
        return self.request(method="GET", path=path)
    
    def request(self, method:str, path:str, payload:dict=None):

        response = requests.request(method=method, url = path, payload= payload)
        try :
            return response.json()
        except Exception as e:
            print(f"Error : {str(e)}")
