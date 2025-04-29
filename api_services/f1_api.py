import requests
from environments.environments import Environments

class AuthoMethodClass(type):

    def __new__(cls, name, bases, dct):

        def get_race_info(self, data:dict, extra_params:dict)->dict:
            data = data["MRData"]["RaceTable"]["Races"][0]["Laps"][0]["Timings"][0]
            return {**data, **extra_params}
        
        def check_race_name(self, data:dict, name:str)->bool:
            if data["MRData"]["RaceTable"]["Races"][0]["Circuit"]["circuitId"].lower() == name.lower():
                return True
            return False
        
        dct["get_race_info"] = get_race_info
        dct["get_race_id"] = check_race_name

        return super().__new__(cls, name, bases, dct)

class F1_API(metaclass = AuthoMethodClass):

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
    
    def request(self, method:str, path:str):

        response = requests.request(method=method, url = path)
        try :
            return response.json()
        except Exception as e:
            print(f"Error : {str(e)}")
