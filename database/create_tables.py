# database/create_tables.py
from sqlalchemy import create_engine
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from schema import Base
from config import DATABASE_URI

def create_tables():
    engine = create_engine(DATABASE_URI)
    Base.metadata.create_all(engine)
    print("Tables created successfully")

if __name__ == "__main__":
    create_tables()