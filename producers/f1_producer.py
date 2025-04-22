#from api_services.f1_api import F1_API
import json
from typing import Dict, List, Iterator
import time
from confluent_kafka import SerializingProducer
import uuid
import requests
#from environments.environments import Environments

class AuthoMethodClass(type):

    def __new__(cls, name, bases, dct):

        def get_race_info(self, data:dict)->dict:
            return data["MRData"]["RaceTable"]["Races"][0]["Laps"][0]["Timings"][0]
        
        def check_race_name(self, data:dict, name:str)->bool:
            if data["MRData"]["RaceTable"]["Races"][0]["Circuit"]["circuitId"].lower() == name.lower():
                return True
            return False
        
        dct["get_race_info"] = get_race_info
        dct["get_race_id"] = check_race_name

        return super().__new__(cls, name, bases, dct)

class F1_API(metaclass = AuthoMethodClass):

    def __init__(self):
        self._base_url = "http://ergast.com/api/f1"
    
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


class F1_Track_range:

    def __init__(self, start:int, end:int):
        self.current = start
        self.end = end
    
    def __iter__(self):
        return self
    
    def __next__(self):
        if self.current <self.end:
            value = self.current
            self.current+=1
            return value
        else:
            raise StopIteration
        
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

if __name__ == "__main__":

    f1_api = F1_API()

    with open("configs/config.json" , mode="r", encoding="utf8") as f:
        configs = json.load(f)

    driver: str = configs.get("driver")
    year: int = configs.get("year")
    circuit: str = configs.get("circuit")
    historical_f1_topic = 'historical_f1_topic'
    f1_tranck_range: F1_Track_range = F1_Track_range(start=1, end=50)
    producer = SerializingProducer({'bootstrap.servers': 'localhost:9092', })

    for i in f1_tranck_range:
        response = f1_api.get_lap_time(driver=driver,year=str(year), race_id=str(i), lap_num=1)
        if f1_api.get_race_id(data=response, name=circuit):
            circuit_id = i
            break
    
    lap = 1
    while True:
        response = f1_api.get_lap_time(driver=driver,year=str(year), race_id=str(circuit_id), lap_num=lap)
        if response["MRData"]["total"]=='0':
            break
        data = f1_api.get_race_info(data = response)
        
        producer.produce(
            historical_f1_topic,
            key=str(uuid.uuid4()),
            value=json.dumps(data),
            on_delivery=delivery_report
        )
        producer.flush()
        lap +=1


    
    

       



