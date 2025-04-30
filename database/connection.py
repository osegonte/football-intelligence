# database/connection.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
import sys
import os

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URI

engine = create_engine(DATABASE_URI)
SessionFactory = sessionmaker(bind=engine)
Session = scoped_session(SessionFactory)

def get_session():
    return Session()

def close_session(session):
    session.close()
    Session.remove()