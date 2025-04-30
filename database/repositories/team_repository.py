# database/repositories/team_repository.py
from .base_repository import BaseRepository
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from database.schema import Team

class TeamRepository(BaseRepository):
    def find_by_id(self, team_id):
        return self.session.query(Team).filter(Team.id == team_id).first()
        
    def find_by_name(self, name):
        return self.session.query(Team).filter(Team.name == name).first()
        
    def find_all(self):
        return self.session.query(Team).all()
        
    def find_by_country(self, country_id):
        return self.session.query(Team).filter(Team.country_id == country_id).all()