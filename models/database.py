from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool
from urllib.parse import quote_plus
from contextlib import contextmanager
from environments.environments import Environments
from typing import List, Iterable, Dict

class Config:

    def __init__(self, engine:str="sqlite", active_directory:bool=False):
        self.username = Environments.get_db_name()
        self.password = Environments.get_db_password()
        self.db_host = Environments.get_db_host()
        self.db_name = Environments.get_db_name()
        self.engine = engine
        self.active_directory = active_directory

    def get_connection_string(self)->str:

        if self.engine=="postgres":
            return f"postgressql://{self.username}:{self.password}@{self.db_host}/{self.db_name}"
        
        elif self.engine in ["sqlserver","mssql"]:
            connection_string =(
                f"Driver={{ODBC Driver 17 for SQL Server}};"
                f"Server={self.db_host};"
                f"DATABASE={self.db_name};ApplicationIntent=ReadOnly;"
            )
            if self.active_directory:
                connection_string+= f"UUID={self.username};Authentication=ActiveDirectoryInteractive;"
            else:
                connection_string+= f"UUID={self.username};PWD={self.password}"
            params = quote_plus(connection_string)

            return f"mssql+pyodbc://?odbc_connect={params}"
        else:
            return f"sqlite:///{self.db_name}.db"

class Database(Config):

    def __init__(self, engine = "sqlite", active_directory = False):
        super().__init__(engine, active_directory)
        self._connnection_string = self.get_connection_string()
        self._db = create_engine(self._connnection_string, pool_pre_ping=True, poolclass=StaticPool)
        self._session = sessionmaker(bind=self._db)
    
    @contextmanager
    def session_scope(self)->Session:
        """Provide a transactional scope around a series of operations"""
        session = self._session()
        try:
            yield session
        except Exception as e:
            session.rollback()
            print(f"Session rollback due to : {e}")
            raise 
        finally:
            session.close()
    
    def get_all_from_model(self, model:object)->List[object]:
        with self.session_scope() as session:
            return session.query(model).all()
    
    def get_all_from_driver(self, model:object, driver:List[str])->List[object]:
        with self.session_scope() as session:
            return session.query(model).filter(model.Driver_id.in_(driver)).all()
    
    def add_data_to_db(self, model:object, data:dict)->None:
        with self.session_scope() as session:
            session.bulk_insert_mappings(model, [data])
            session.commit()
    
    def delete_row_from_db(self, model:object, id_list:List[int])->None:
        with self.session_scope() as session:
            for item in id_list:
                to_remove= session.query(model).filter_by(id=item).first()
                if to_remove:
                    session.delete(to_remove)
                    session.commit()
