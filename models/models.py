from sqlalchemy import Column, String, Integer, Float
from sqlalchemy.orm import sessionmaker, Session, declarative_base, relationship
from sqlalchemy import create_engine
from models.database import Config

EntityDboBase = declarative_base()

class F1Model(EntityDboBase):

    __tablename__ = "driver_results_table"
    
    id = Column(Integer, primary_key=True)
    Driver_id = Column(String)
    Track = Column(String)
    Time = Column(String)
    Lap = Column(String)
    Position = Column(String)
    Year = Column(Integer)

    def __init__(self, **kwargs)->None:
        self.Driver_id = kwargs.get("driverId")
        self.Track  = kwargs.get("track")
        self.Time = kwargs.get("time")
        self.Lap = kwargs.get("lap")
        self.Position = kwargs.get("position")
        self.Year = kwargs.get("year")

    def __str__(self)->str:
        return  "Driver: {self.Driver_id} is current in positon : {self.Position} in Lap: {self.Lap}"
    
config = Config()
engine = create_engine(config.get_connection_string())
EntityDboBase.metadata.create_all(bind=engine)
