from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base

def get_db_connection():
   server = "testazuredbs.database.windows.net"
   database = "testDB"
   username = "adminsql"
   password = "Freeforever280*"
   database_url = (
        f"mssql+pyodbc://{username}:{password}@{server}/{database}?"
        "driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
    )
   #alchemy config
   engine = create_engine(database_url)
   SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

   return engine, SessionLocal

def init_db(engine):
    """Init DB."""
    Base.metadata.create_all(bind=engine)
