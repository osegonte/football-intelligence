#!/usr/bin/env python3
# quick_migrate.py - Standalone script to import match data
import os
import sys
import pandas as pd
from datetime import datetime
import traceback

# Database connection setup
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

# Get database URI from your config
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
try:
    from config import DATABASE_URI
    print(f"Using database: {DATABASE_URI}")
except ImportError:
    print("Error: Could not import DATABASE_URI from config.py")
    sys.exit(1)

# Define a simplified schema without the problematic TeamStats relationship
Base = declarative_base()

class Country(Base):
    __tablename__ = 'countries'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False, unique=True)
    region = Column(String(100))
    
    leagues = relationship("League", back_populates="country")
    teams = relationship("Team", back_populates="country")

class League(Base):
    __tablename__ = 'leagues'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    country_id = Column(Integer, ForeignKey('countries.id'))
    
    country = relationship("Country", back_populates="leagues")
    fixtures = relationship("Fixture", back_populates="league")

class Team(Base):
    __tablename__ = 'teams'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    country_id = Column(Integer, ForeignKey('countries.id'))
    short_name = Column(String(50))
    logo_url = Column(String(255))
    
    country = relationship("Country", back_populates="teams")
    home_fixtures = relationship("Fixture", foreign_keys="Fixture.home_team_id", back_populates="home_team")
    away_fixtures = relationship("Fixture", foreign_keys="Fixture.away_team_id", back_populates="away_team")

class Fixture(Base):
    __tablename__ = 'fixtures'
    
    id = Column(Integer, primary_key=True)
    external_id = Column(String(100), unique=True)
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
    
    home_team = relationship("Team", foreign_keys=[home_team_id], back_populates="home_fixtures")
    away_team = relationship("Team", foreign_keys=[away_team_id], back_populates="away_fixtures")
    league = relationship("League", back_populates="fixtures")

def setup_database():
    """Set up the database tables"""
    print("Creating database tables...")
    try:
        engine = create_engine(DATABASE_URI)
        
        # Check if we can connect
        connection = engine.connect()
        connection.close()
        print("Successfully connected to database")
        
        # Drop tables if they exist
        print("Dropping existing tables...")
        Base.metadata.drop_all(engine)
        
        # Create tables
        print("Creating new tables...")
        Base.metadata.create_all(engine)
        
        print("Database setup complete")
        return engine
    except Exception as e:
        print(f"Database setup error: {str(e)}")
        traceback.print_exc()
        sys.exit(1)

def import_csv_data(csv_path, session):
    """Import data from CSV file into the database"""
    print(f"Reading CSV file: {csv_path}")
    try:
        df = pd.read_csv(csv_path)
        print(f"CSV loaded successfully. Shape: {df.shape}")
        print(f"Columns: {df.columns.tolist()}")
        print(f"Sample row: {df.iloc[0].to_dict()}")
    except Exception as e:
        print(f"Error reading CSV: {str(e)}")
        traceback.print_exc()
        return 0
    
    # Process countries
    print("Processing countries...")
    unique_countries = df['country'].dropna().unique()
    print(f"Found {len(unique_countries)} unique countries")
    country_map = {}
    
    for country_name in unique_countries:
        country = Country(name=country_name)
        session.add(country)
        try:
            session.flush()
            country_map[country_name] = country.id
        except Exception as e:
            session.rollback()
            print(f"Error adding country {country_name}: {str(e)}")
            # Try to get existing country
            country = session.query(Country).filter(Country.name == country_name).first()
            if country:
                country_map[country_name] = country.id
    
    session.commit()
    print(f"Processed {len(country_map)} countries")
    
    # Process leagues
    print("Processing leagues...")
    unique_leagues = df[['league', 'country']].dropna().drop_duplicates().values.tolist()
    print(f"Found {len(unique_leagues)} unique leagues")
    league_map = {}
    
    for league_name, country_name in unique_leagues:
        if country_name not in country_map:
            print(f"Warning: Country not found for league {league_name}: {country_name}")
            continue
            
        league_key = f"{league_name}_{country_name}"
        league = League(
            name=league_name,
            country_id=country_map[country_name]
        )
        
        session.add(league)
        try:
            session.flush()
            league_map[league_key] = league.id
        except Exception as e:
            session.rollback()
            print(f"Error adding league {league_name}: {str(e)}")
            # Try to get existing league
            league = session.query(League).filter(
                League.name == league_name,
                League.country_id == country_map[country_name]
            ).first()
            if league:
                league_map[league_key] = league.id
    
    session.commit()
    print(f"Processed {len(league_map)} leagues")
    
    # Process teams
    print("Processing teams...")
    all_teams = set()
    for _, row in df.iterrows():
        if pd.notna(row.get('home_team')):
            all_teams.add(row['home_team'])
        if pd.notna(row.get('away_team')):
            all_teams.add(row['away_team'])
    
    print(f"Found {len(all_teams)} unique teams")
    team_map = {}
    
    for team_name in all_teams:
        # Find most common country for this team
        team_matches = df[(df['home_team'] == team_name) | (df['away_team'] == team_name)]
        if team_matches.empty:
            print(f"Warning: No matches found for team {team_name}")
            continue
            
        most_common_country = team_matches['country'].mode()[0]
        if most_common_country not in country_map:
            print(f"Warning: Country not found for team {team_name}: {most_common_country}")
            continue
            
        team = Team(
            name=team_name,
            country_id=country_map[most_common_country]
        )
        
        session.add(team)
        try:
            session.flush()
            team_map[team_name] = team.id
        except Exception as e:
            session.rollback()
            print(f"Error adding team {team_name}: {str(e)}")
            # Try to get existing team
            team = session.query(Team).filter(Team.name == team_name).first()
            if team:
                team_map[team_name] = team.id
    
    session.commit()
    print(f"Processed {len(team_map)} teams")
    
    # Process fixtures
    print("Processing fixtures...")
    fixtures_added = 0
    batch_size = 100
    
    for idx, row in df.iterrows():
        # Check for required fields
        if pd.isna(row.get('date')) or pd.isna(row.get('home_team')) or pd.isna(row.get('away_team')):
            continue
            
        # Convert date string to datetime
        try:
            match_date = pd.to_datetime(row['date']).to_pydatetime()
        except Exception as e:
            print(f"Error parsing date for row {idx}: {str(e)}")
            continue
        
        # Get team IDs
        home_team_name = row['home_team']
        away_team_name = row['away_team']
        league_name = row['league']
        country_name = row['country']
        
        if home_team_name not in team_map:
            print(f"Warning: Home team not found: {home_team_name}")
            continue
            
        if away_team_name not in team_map:
            print(f"Warning: Away team not found: {away_team_name}")
            continue
            
        league_key = f"{league_name}_{country_name}"
        if league_key not in league_map:
            print(f"Warning: League not found: {league_key}")
            continue
            
        # Create new fixture
        fixture = Fixture(
            external_id=str(row.get('id', f'gen_{idx}')),
            home_team_id=team_map[home_team_name],
            away_team_id=team_map[away_team_name],
            league_id=league_map[league_key],
            date=match_date,
            start_time=str(row.get('start_time', '')) if pd.notna(row.get('start_time')) else None,
            venue=str(row.get('venue', '')) if pd.notna(row.get('venue')) else None,
            status=str(row.get('status', '')) if pd.notna(row.get('status')) else None,
            round=str(row.get('round', '')) if pd.notna(row.get('round')) else None,
            source=str(row.get('source', '')) if pd.notna(row.get('source')) else None,
            scrape_date=datetime.now()
        )
        
        session.add(fixture)
        fixtures_added += 1
        
        # Commit in batches
        if fixtures_added % batch_size == 0:
            try:
                session.commit()
                print(f"Imported {fixtures_added} fixtures...")
            except Exception as e:
                session.rollback()
                print(f"Error batch importing fixtures: {str(e)}")
    
    # Final commit
    try:
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error on final commit: {str(e)}")
        
    print(f"Successfully imported {fixtures_added} fixtures from {csv_path}")
    return fixtures_added

def main():
    print("Starting quick database migration...")
    
    # Check if CSV file provided
    if len(sys.argv) < 2:
        print("Usage: python quick_migrate.py <csv_file>")
        return 1
        
    csv_path = sys.argv[1]
    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found: {csv_path}")
        return 1
        
    # Set up database
    engine = setup_database()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Import data
        import_csv_data(csv_path, session)
        print("Database migration completed successfully!")
        return 0
    except Exception as e:
        print(f"Error: {str(e)}")
        traceback.print_exc()
        return 1
    finally:
        session.close()

if __name__ == "__main__":
    sys.exit(main())