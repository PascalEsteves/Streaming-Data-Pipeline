from api_services.f1_api import F1_API
import json
from typing import Dict, List, Iterator
from confluent_kafka import SerializingProducer
import uuid
import random
from models.models import F1Model
from models.database import Database

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

def convert_seconds_to_time(seconds):
    minutes = int(seconds // 60)
    sec = seconds % 60
    return f"{minutes}:{sec:06.3f}"

def run():
    f1_api = F1_API()
    db = Database()

    with open("configs/config.json" , mode="r", encoding="utf8") as f:
        configs = json.load(f)

    # Variables of Driver, Years and Circuit to simulate
    driver: str = configs.get("driver")
    years: list = configs.get("year")
    circuit: str = configs.get("circuit")
    time_delta: list = range(80,100)
    current_year = years[-1]+1
    positions = range(1,10)

    # Kafka Topics
    historical_f1_topic = 'historical_f1_topic'
    real_time_topic = "real_time_f1_topic"
    producer = SerializingProducer({'bootstrap.servers': 'localhost:9092', })

    f1_track_range: F1_Track_range = F1_Track_range(start=1, end=70)
    
    for i in f1_track_range:
        response = f1_api.get_lap_time(driver=driver,year=str(2024), race_id=str(i), lap_num=1)
        if f1_api.get_race_id(data=response, name=circuit):
            circuit_id = i
            break
    
    lap = 1
    while True:
        print("LAP ------- ", lap)
        should_break = False
        
        for year in years:
            response = f1_api.get_lap_time(driver=driver, year=str(year), race_id=str(circuit_id), lap_num=lap)
            if response["MRData"]["total"] == '0':
                should_break = True
                break

            extra_data = {
                "year": year,
                "lap": lap,
                "track": circuit
            }
            data = f1_api.get_race_info(data=response, extra_params=extra_data)
            print(data)
            db.add_data_to_db(model=F1Model, data=F1Model(**data).__dict__)
            producer.produce(
                historical_f1_topic,
                key=f"{str(year)}-{str(uuid.uuid4())}",
                value=json.dumps(data),
                on_delivery=delivery_report
            )
        
        if should_break:
            break

        real_data = {
            "driverId": driver.lower(),
            "position": str(random.choice(positions)),
            "time": convert_seconds_to_time(random.choice(time_delta)),
            "year": current_year,
            "lap": lap,
            "track": circuit
        }
        print(real_data)
        db.add_data_to_db(model=F1Model, data=F1Model(**real_data).__dict__)
        producer.produce(
            historical_f1_topic,
            key=f"{str(current_year)}-{str(uuid.uuid4())}",
            value=json.dumps(real_data),
            on_delivery=delivery_report
        )

        # Garante que todos os dados foram enviados antes de passar para a próxima volta
        producer.flush()
        lap += 1


if __name__ == "__main__":
    run()
