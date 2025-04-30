#!/usr/bin/env python3
"""
Improved CSV to Database Migration Script for Football Intelligence platform.
This script provides better error handling, logging, and debugging features.

Usage:
    python migrate_fix.py <csv_file_path>
"""
import os
import sys
import pandas as pd
import traceback
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('database_migration.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Make sure we're in the right directory
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# Add parent directory to path
sys.path.insert(0, script_dir)

# Import database modules with proper error handling
try:
    from config import DATABASE_URI
    from sqlalchemy import create_engine, inspect, text
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.ext.declarative import declarative_base
    Base = declarative_base()
    
    # Try to import the schema
    try:
        from database.schema import Country, League, Team, Fixture, Base
        SCHEMA_IMPORTED = True
        logger.info("Successfully imported database schema")
    except ImportError:
        SCHEMA_IMPORTED = False
        logger.error("Failed to import database schema, using simplified schema")
        
        # Define a simplified schema
        from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
        from sqlalchemy.orm import relationship
        
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
except ImportError as e:
    logger.error(f"Failed to import required modules: {str(e)}")
    logger.error("Please install required packages: sqlalchemy, psycopg2-binary")
    sys.exit(1)

def setup_database(drop_tables=False):
    """Set up database connection and tables"""
    logger.info(f"Connecting to database: {DATABASE_URI}")
    
    try:
        # Create engine and test connection
        engine = create_engine(DATABASE_URI)
        connection = engine.connect()
        connection.close()
        logger.info("Successfully connected to database")
        
        # Check if tables exist
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()
        logger.info(f"Existing tables: {existing_tables}")
        
        # Drop tables if requested
        if drop_tables:
            logger.warning("Dropping all existing tables (THIS WILL DELETE ALL DATA)")
            Base.metadata.drop_all(engine)
            logger.info("Tables dropped successfully")
        
        # Create tables if they don't exist
        if not all(table in existing_tables for table in ['countries', 'leagues', 'teams', 'fixtures']):
            logger.info("Creating missing tables")
            Base.metadata.create_all(engine)
            logger.info("Tables created successfully")
        
        # Create session
        Session = sessionmaker(bind=engine)
        session = Session()
        
        return engine, session
    except Exception as e:
        logger.error(f"Database setup error: {str(e)}")
        traceback.print_exc()
        sys.exit(1)

def get_csv_file_info(csv_path):
    """Get information about the CSV file"""
    try:
        df = pd.read_csv(csv_path)
        logger.info(f"CSV file loaded successfully:")
        logger.info(f"  - Dimensions: {df.shape}")
        logger.info(f"  - Columns: {df.columns.tolist()}")
        logger.info(f"  - Sample row: {df.iloc[0].to_dict()}")
        
        # Check for required columns
        required_columns = ['home_team', 'away_team', 'league', 'country', 'date']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return None
            
        return df
    except Exception as e:
        logger.error(f"Error reading CSV: {str(e)}")
        traceback.print_exc()
        return None

def import_data(df, session, batch_size=50):
    """Import data from DataFrame to database"""
    try:
        # Process countries
        logger.info("Processing countries")
        unique_countries = df['country'].dropna().unique()
        logger.info(f"Found {len(unique_countries)} unique countries")
        country_map = {}
        
        for country_name in unique_countries:
            try:
                # Check if country exists
                country = session.query(Country).filter(Country.name == country_name).first()
                if not country:
                    country = Country(name=country_name)
                    session.add(country)
                    session.flush()
                    logger.info(f"Added country: {country_name}")
                country_map[country_name] = country.id
            except Exception as e:
                logger.error(f"Error processing country {country_name}: {str(e)}")
                session.rollback()
        
        session.commit()
        logger.info(f"Processed {len(country_map)} countries")
        
        # Process leagues
        logger.info("Processing leagues")
        unique_leagues = df[['league', 'country']].dropna().drop_duplicates().values.tolist()
        logger.info(f"Found {len(unique_leagues)} unique leagues")
        league_map = {}
        
        for league_name, country_name in unique_leagues:
            if country_name not in country_map:
                logger.warning(f"Country not found for league {league_name}: {country_name}")
                continue
                
            league_key = f"{league_name}_{country_name}"
            try:
                # Check if league exists
                league = session.query(League).filter(
                    League.name == league_name,
                    League.country_id == country_map[country_name]
                ).first()
                
                if not league:
                    league = League(
                        name=league_name,
                        country_id=country_map[country_name]
                    )
                    session.add(league)
                    session.flush()
                    logger.info(f"Added league: {league_name} ({country_name})")
                    
                league_map[league_key] = league.id
            except Exception as e:
                logger.error(f"Error processing league {league_name}: {str(e)}")
                session.rollback()
        
        session.commit()
        logger.info(f"Processed {len(league_map)} leagues")
        
        # Process teams
        logger.info("Processing teams")
        all_teams = set()
        for _, row in df.iterrows():
            if pd.notna(row.get('home_team')):
                all_teams.add(row['home_team'])
            if pd.notna(row.get('away_team')):
                all_teams.add(row['away_team'])
        
        logger.info(f"Found {len(all_teams)} unique teams")
        team_map = {}
        
        for team_name in all_teams:
            try:
                # Check if team exists
                team = session.query(Team).filter(Team.name == team_name).first()
                
                if not team:
                    # Find most common country for this team
                    team_matches = df[(df['home_team'] == team_name) | (df['away_team'] == team_name)]
                    if team_matches.empty:
                        logger.warning(f"No matches found for team {team_name}")
                        continue
                        
                    most_common_country = team_matches['country'].mode()[0]
                    if most_common_country not in country_map:
                        logger.warning(f"Country not found for team {team_name}: {most_common_country}")
                        continue
                        
                    team = Team(
                        name=team_name,
                        country_id=country_map[most_common_country]
                    )
                    session.add(team)
                    session.flush()
                    logger.info(f"Added team: {team_name}")
                    
                team_map[team_name] = team.id
            except Exception as e:
                logger.error(f"Error processing team {team_name}: {str(e)}")
                session.rollback()
        
        session.commit()
        logger.info(f"Processed {len(team_map)} teams")
        
        # Process fixtures
        logger.info("Processing fixtures")
        fixtures_added = 0
        errors = 0
        
        # Convert date column to datetime if it's not already
        if 'date' in df.columns and df['date'].dtype == 'object':
            df['date'] = pd.to_datetime(df['date'])
        
        # Process in batches
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            
            for _, row in batch.iterrows():
                try:
                    # Check for required fields
                    if pd.isna(row.get('date')) or pd.isna(row.get('home_team')) or pd.isna(row.get('away_team')):
                        logger.warning(f"Skipping row with missing required fields: {row.name}")
                        continue
                        
                    # Convert date string to datetime
                    try:
                        match_date = pd.to_datetime(row['date']).to_pydatetime()
                    except Exception as e:
                        logger.error(f"Error parsing date for row {row.name}: {str(e)}")
                        continue
                    
                    # Get team IDs
                    home_team_name = row['home_team']
                    away_team_name = row['away_team']
                    league_name = row['league']
                    country_name = row['country']
                    
                    if home_team_name not in team_map:
                        logger.warning(f"Home team not found: {home_team_name}")
                        continue
                        
                    if away_team_name not in team_map:
                        logger.warning(f"Away team not found: {away_team_name}")
                        continue
                        
                    league_key = f"{league_name}_{country_name}"
                    if league_key not in league_map:
                        logger.warning(f"League not found: {league_key}")
                        continue
                    
                    # Get unique ID for fixture
                    external_id = str(row.get('id', f'gen_{row.name}'))
                    
                    # Check if fixture already exists
                    existing_fixture = session.query(Fixture).filter(Fixture.external_id == external_id).first()
                    if existing_fixture:
                        #logger.debug(f"Fixture already exists: {external_id}")
                        continue
                        
                    # Create new fixture
                    fixture = Fixture(
                        external_id=external_id,
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
                    
                except Exception as e:
                    logger.error(f"Error processing fixture at row {row.name}: {str(e)}")
                    errors += 1
                    continue
            
            # Commit batch
            try:
                session.commit()
                logger.info(f"Committed batch: {i} to {i+len(batch)} ({fixtures_added} fixtures so far)")
            except Exception as e:
                logger.error(f"Error committing batch: {str(e)}")
                session.rollback()
                errors += len(batch)
        
        # Report results
        if fixtures_added > 0:
            logger.info(f"Successfully imported {fixtures_added} fixtures")
        else:
            logger.warning("No new fixtures were imported")
            
        if errors > 0:
            logger.warning(f"Encountered {errors} errors during import")
            
        return fixtures_added, errors
        
    except Exception as e:
        logger.error(f"Error during data import: {str(e)}")
        traceback.print_exc()
        session.rollback()
        return 0, -1

def main():
    logger.info("=== Improved CSV to Database Migration Tool ===")
    
    # Check arguments
    if len(sys.argv) < 2:
        logger.error("Usage: python migrate_fix.py <csv_file_path> [--drop-tables]")
        return 1
    
    # Parse arguments
    csv_path = sys.argv[1]
    drop_tables = "--drop-tables" in sys.argv
    
    # Check if CSV file exists
    if not os.path.exists(csv_path):
        logger.error(f"CSV file not found: {csv_path}")
        return 1
    
    # Get CSV file info
    df = get_csv_file_info(csv_path)
    if df is None:
        return 1
    
    # Setup database
    engine, session = setup_database(drop_tables)
    
    try:
        # Import data
        fixtures_added, errors = import_data(df, session)
        
        if fixtures_added > 0:
            logger.info("✅ Database migration completed successfully!")
            return 0
        elif errors < 0:
            logger.error("❌ Database migration failed!")
            return 1
        else:
            logger.warning("⚠️ Database migration completed, but no new fixtures were added")
            return 0
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        traceback.print_exc()
        return 1
    finally:
        session.close()

if __name__ == "__main__":
    sys.exit(main())