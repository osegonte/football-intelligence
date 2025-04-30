# database/schema.py
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Country(Base):
    __tablename__ = 'countries'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False, unique=True)
    region = Column(String(100))
    
    # Relationships
    leagues = relationship("League", back_populates="country")
    teams = relationship("Team", back_populates="country")

class Season(Base):
    __tablename__ = 'seasons'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    
    # Relationships
    leagues = relationship("League", back_populates="season")

class League(Base):
    __tablename__ = 'leagues'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    country_id = Column(Integer, ForeignKey('countries.id'))
    season_id = Column(Integer, ForeignKey('seasons.id'))
    tier = Column(Integer)  # League tier/division (1 for top league)
    
    # Relationships
    country = relationship("Country", back_populates="leagues")
    season = relationship("Season", back_populates="leagues")
    fixtures = relationship("Fixture", back_populates="league")

class Team(Base):
    __tablename__ = 'teams'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    country_id = Column(Integer, ForeignKey('countries.id'))
    short_name = Column(String(50))
    logo_url = Column(String(255))
    
    # Relationships
    country = relationship("Country", back_populates="teams")
    home_fixtures = relationship("Fixture", foreign_keys="Fixture.home_team_id", back_populates="home_team")
    away_fixtures = relationship("Fixture", foreign_keys="Fixture.away_team_id", back_populates="away_team")
    stats = relationship("TeamStats", back_populates="team")

class Fixture(Base):
    __tablename__ = 'fixtures'
    
    id = Column(Integer, primary_key=True)
    external_id = Column(String(100), unique=True)  # ID from data source (SofaScore, FBref)
    home_team_id = Column(Integer, ForeignKey('teams.id'))
    away_team_id = Column(Integer, ForeignKey('teams.id'))
    league_id = Column(Integer, ForeignKey('leagues.id'))
    date = Column(DateTime, nullable=False)
    start_time = Column(String(10))
    venue = Column(String(255))
    status = Column(String(50))
    round = Column(String(50))
    source = Column(String(50))
    scrape_date = Column(DateTime)
    
    # Relationships
    home_team = relationship("Team", foreign_keys=[home_team_id], back_populates="home_fixtures")
    away_team = relationship("Team", foreign_keys=[away_team_id], back_populates="away_fixtures")
    league = relationship("League", back_populates="fixtures")
    stats = relationship("TeamStats", back_populates="fixture")

class TeamStats(Base):
    __tablename__ = 'team_stats'
    
    id = Column(Integer, primary_key=True)
    team_id = Column(Integer, ForeignKey('teams.id'))
    fixture_id = Column(Integer, ForeignKey('fixtures.id'))
    opponent_id = Column(Integer, ForeignKey('teams.id'))
    is_home = Column(Boolean)
    date = Column(DateTime)
    
    # Basic stats
    goals_for = Column(Integer)
    goals_against = Column(Integer)
    xg = Column(Float)  # Expected goals
    shots = Column(Integer)
    shots_on_target = Column(Integer)
    possession = Column(Float)
    
    # Advanced stats
    passes = Column(Integer)
    pass_accuracy = Column(Float)
    tackles = Column(Integer)
    corners = Column(Integer)
    fouls = Column(Integer)
    yellow_cards = Column(Integer)
    red_cards = Column(Integer)
    
    # Additional stats for FBref data
    avg_shot_distance = Column(Float)
    free_kicks = Column(Integer)
    penalties_scored = Column(Integer)
    penalties_attempted = Column(Integer)
    
    # Metadata
    source = Column(String(50))
    scrape_date = Column(DateTime)
    
    # Relationships
    team = relationship("Team", foreign_keys=[team_id], back_populates="stats")
    opponent = relationship("Team", foreign_keys=[opponent_id])
    fixture = relationship("Fixture", back_populates="stats")