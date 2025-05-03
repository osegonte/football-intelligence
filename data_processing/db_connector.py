#!/usr/bin/env python3
"""
Database Connector for Football Intelligence Project

This module connects to the existing PostgreSQL database schema
and stores only reliable data from FBref (basic match statistics)
with default values for advanced stats to maintain compatibility
"""
import os
import logging
import pandas as pd
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import configparser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('database.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FootballDBConnector:
    """
    Database connector for the Football Intelligence project.
    Stores only reliable data while maintaining compatibility with existing schema.
    """
    
    def __init__(self, config_file='config.ini'):
        """Initialize the database connector with configuration from a file."""
        self.config = self._load_config(config_file)
        self.conn = None
        self.cur = None
    
    def _load_config(self, config_file):
        """Load database configuration from file"""
        # Default configuration
        config = {
            "dbname": "fbref_stats",
            "user": "scraper_user",
            "password": "1759",
            "host": "localhost",
            "port": 5432
        }
        
        # Try to load configuration from file
        if os.path.exists(config_file):
            try:
                parser = configparser.ConfigParser()
                parser.read(config_file)
                
                if 'database' in parser:
                    for key in ['dbname', 'user', 'password', 'host']:
                        if key in parser['database']:
                            config[key] = parser['database'][key]
                    
                    if 'port' in parser['database']:
                        config['port'] = int(parser['database']['port'])
                
                logger.info(f"Loaded database configuration from {config_file}")
            except Exception as e:
                logger.error(f"Error loading config file: {str(e)}")
        else:
            logger.warning(f"Config file {config_file} not found, using defaults")
        
        return config
    
    def connect(self):
        """Connect to the PostgreSQL database."""
        try:
            self.conn = psycopg2.connect(
                dbname=self.config['dbname'],
                user=self.config['user'],
                password=self.config['password'],
                host=self.config['host'],
                port=self.config['port']
            )
            self.cur = self.conn.cursor()
            
            logger.info(f"Connected to PostgreSQL database: {self.config['dbname']}")
            return True
            
        except Exception as e:
            logger.error(f"Error connecting to database: {str(e)}")
            return False
    
    def disconnect(self):
        """Close the database connection."""
        if self.cur:
            self.cur.close()
            self.cur = None
        
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("Database connection closed")
    
    def execute_query(self, query, params=None, commit=True):
        """Execute a SQL query and return results"""
        if not self.conn or not self.cur:
            if not self.connect():
                return None
        
        try:
            if params:
                self.cur.execute(query, params)
            else:
                self.cur.execute(query)
            
            if commit and query.strip().upper().startswith(("INSERT", "UPDATE", "DELETE")):
                self.conn.commit()
            
            if query.strip().upper().startswith("SELECT"):
                return self.cur.fetchall()
            
            return True
            
        except Exception as e:
            if self.conn:
                self.conn.rollback()
            logger.error(f"Error executing query: {str(e)}")
            return None
    
    def execute_values(self, query, values, commit=True):
        """Execute a query with multiple sets of values"""
        if not self.conn or not self.cur:
            if not self.connect():
                return False
        
        try:
            execute_values(self.cur, query, values)
            
            if commit:
                self.conn.commit()
            
            return True
            
        except Exception as e:
            if self.conn:
                self.conn.rollback()
            logger.error(f"Error executing values: {str(e)}")
            return False
    
    def get_or_create_team(self, team_name):
        """Get or create a team in the database."""
        if not self.connect():
            return None
        
        try:
            # Check if team exists
            query = "SELECT id FROM teams WHERE name = %s"
            result = self.execute_query(query, (team_name,))
            
            if result and result[0][0]:
                return result[0][0]
            
            # Create team
            query = "INSERT INTO teams (name) VALUES (%s) RETURNING id"
            result = self.execute_query(query, (team_name,))
            
            if result and result[0][0]:
                logger.info(f"Created new team: {team_name}")
                return result[0][0]
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting/creating team: {str(e)}")
            return None
    
    def get_or_create_competition(self, competition_name):
        """Get or create a competition in the database."""
        if not self.connect():
            return None
        
        try:
            # Check if competition exists
            query = "SELECT id FROM competitions WHERE name = %s"
            result = self.execute_query(query, (competition_name,))
            
            if result and result[0][0]:
                return result[0][0]
            
            # Create competition
            query = "INSERT INTO competitions (name) VALUES (%s) RETURNING id"
            result = self.execute_query(query, (competition_name,))
            
            if result and result[0][0]:
                logger.info(f"Created new competition: {competition_name}")
                return result[0][0]
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting/creating competition: {str(e)}")
            return None
    
    def get_or_create_venue(self, venue_name):
        """Get or create a venue in the database."""
        if not venue_name:
            venue_name = "Unknown"
            
        if not self.connect():
            return None
        
        try:
            # Check if venue exists
            query = "SELECT id FROM venues WHERE name = %s"
            result = self.execute_query(query, (venue_name,))
            
            if result and result[0][0]:
                return result[0][0]
            
            # Create venue
            query = "INSERT INTO venues (name) VALUES (%s) RETURNING id"
            result = self.execute_query(query, (venue_name,))
            
            if result and result[0][0]:
                logger.info(f"Created new venue: {venue_name}")
                return result[0][0]
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting/creating venue: {str(e)}")
            return None
    
    def get_or_create_season(self, date_str=None):
        """Get or create a season based on the date."""
        if not date_str:
            date_str = datetime.now().strftime('%Y-%m-%d')
            
        if not self.connect():
            return None
        
        try:
            # Convert date string to datetime
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            
            # Determine season label
            if date_obj.month >= 7:
                season_label = f"{date_obj.year}/{str(date_obj.year + 1)[2:]}"
            else:
                season_label = f"{date_obj.year - 1}/{str(date_obj.year)[2:]}"
            
            # Check if season exists
            query = "SELECT id FROM seasons WHERE label = %s"
            result = self.execute_query(query, (season_label,))
            
            if result and result[0][0]:
                return result[0][0]
            
            # Create season
            query = "INSERT INTO seasons (label) VALUES (%s) RETURNING id"
            result = self.execute_query(query, (season_label,))
            
            if result and result[0][0]:
                logger.info(f"Created new season: {season_label}")
                return result[0][0]
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting/creating season: {str(e)}")
            return None
    
    def store_match(self, match_data):
        """Store a match in the database with default values for advanced stats."""
        if not self.connect():
            return None
        
        try:
            # Get or create related entities
            home_team_id = self.get_or_create_team(match_data.get('team') if match_data.get('venue') == 'Home' else match_data.get('opponent'))
            away_team_id = self.get_or_create_team(match_data.get('opponent') if match_data.get('venue') == 'Home' else match_data.get('team'))
            competition_id = self.get_or_create_competition(match_data.get('comp', 'Unknown'))
            venue_id = self.get_or_create_venue(match_data.get('venue', 'Unknown'))
            season_id = self.get_or_create_season(match_data.get('date').strftime('%Y-%m-%d') if isinstance(match_data.get('date'), datetime) else match_data.get('date'))
            
            if None in [home_team_id, away_team_id, competition_id, venue_id, season_id]:
                logger.error("Failed to get or create related entities")
                return None
            
            # Determine goals based on perspective
            if match_data.get('venue') == 'Home':
                home_goals = match_data.get('gf', 0)
                away_goals = match_data.get('ga', 0)
            else:
                home_goals = match_data.get('ga', 0)
                away_goals = match_data.get('gf', 0)
            
            # Check if match exists
            query = """
                SELECT id FROM matches 
                WHERE date = %s 
                AND home_team_id = %s 
                AND away_team_id = %s 
                AND competition_id = %s
            """
            result = self.execute_query(query, (
                match_data.get('date'),
                home_team_id,
                away_team_id,
                competition_id
            ))
            
            if result and result[0][0]:
                # Match exists, update it
                match_id = result[0][0]
                
                query = """
                    UPDATE matches SET 
                        venue_id = %s,
                        season_id = %s,
                        home_goals = %s,
                        away_goals = %s,
                        round = %s
                    WHERE id = %s
                """
                
                self.execute_query(query, (
                    venue_id,
                    season_id,
                    home_goals,
                    away_goals,
                    match_data.get('round'),
                    match_id
                ))
                
                logger.info(f"Updated match: {match_data.get('match_id')}")
                return match_id
            
            # Create new match
            query = """
                INSERT INTO matches (
                    competition_id, season_id, date, venue_id,
                    home_team_id, away_team_id, 
                    home_goals, away_goals, round,
                    home_xg, away_xg,
                    home_shots, away_shots,
                    home_sot, away_sot,
                    home_possession, away_possession,
                    home_corners, away_corners
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """
            
            # Set default values for advanced stats
            result = self.execute_query(query, (
                competition_id,
                season_id,
                match_data.get('date'),
                venue_id,
                home_team_id,
                away_team_id,
                home_goals,
                away_goals,
                match_data.get('round'),
                0.0,  # home_xg
                0.0,  # away_xg
                0,    # home_shots
                0,    # away_shots
                0,    # home_sot
                0,    # away_sot
                0.0,  # home_possession
                0.0,  # away_possession
                0,    # home_corners
                0     # away_corners
            ))
            
            if result and result[0][0]:
                match_id = result[0][0]
                logger.info(f"Created new match: {match_data.get('match_id')}")
                return match_id
            
            return None
            
        except Exception as e:
            logger.error(f"Error storing match: {str(e)}")
            return None
        finally:
            self.disconnect()
    
    def store_matches_from_dataframe(self, df):
        """Store matches from a pandas DataFrame."""
        if df.empty:
            logger.warning("Empty DataFrame, no matches to store")
            return 0
        
        success_count = 0
        
        for _, row in df.iterrows():
            match_id = self.store_match(row.to_dict())
            
            if match_id:
                success_count += 1
        
        return success_count