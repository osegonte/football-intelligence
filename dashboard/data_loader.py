# dashboard/data_loader.py
"""
Data loader module for the Football Intelligence dashboard.
Supports both CSV files and PostgreSQL database as data sources.
"""
import os
import pandas as pd
from datetime import datetime, timedelta
import logging
import streamlit as st
import sys

# Add parent directory to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

# Import the database connector
from data_processing.db_connector import DatabaseConnector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

class FootballDataLoader:
    """
    Data loader for football match data using either CSV storage or PostgreSQL database.
    """
    
    def __init__(self, use_db=False, db_config=None):
        """
        Initialize the data loader.
        
        Args:
            use_db: Whether to use the database instead of CSV files
            db_config: Dictionary with database connection parameters
        """
        # Cached data
        self._fixtures = None
        self._leagues = None
        self._countries = None
        self._teams = None
        
        # Database settings
        self.use_db = use_db
        self.db = None
        self.data_source = "CSV"
        
        if use_db:
            try:
                # Initialize database connection
                if db_config is None:
                    db_config = {}
                
                self.db = DatabaseConnector(**db_config)
                
                if self.db.test_connection():
                    self.data_source = f"PostgreSQL ({self.db.dbname})"
                    logger.info(f"Using database source: {self.data_source}")
                else:
                    logger.warning("Database connection failed, falling back to CSV")
                    self.use_db = False
                    self.db = None
            except Exception as e:
                logger.error(f"Error connecting to database: {str(e)}")
                self.use_db = False
                self.db = None
    
    def _get_csv_path(self):
        """Get path to the fixtures CSV file"""
        script_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(script_dir)
        
        # Try different potential locations
        candidate_paths = [
            os.path.join(parent_dir, "data", "all_matches_latest.csv"),
            os.path.join(parent_dir, "sofascore_data", "all_matches_latest.csv")
        ]
        
        for path in candidate_paths:
            if os.path.exists(path):
                return path
        
        return None
    
    def _load_from_database(self):
        """
        Load fixtures data from the database
        
        Returns:
            DataFrame with fixtures data
        """
        if not self.db:
            raise ValueError("Database connection not initialized")
        
        try:
            # Connect to database
            if not self.db.connect():
                raise ConnectionError("Failed to connect to database")
            
            # Query to get all matches with team and league info
            query = """
            SELECT 
                m.match_id AS id,
                m.date,
                home.team_name AS home_team,
                away.team_name AS away_team,
                l.league_name AS league,
                -- Using a common country field for simplicity
                (SELECT league_name FROM league WHERE league_id = 
                    (SELECT league_id FROM team WHERE team_id = m.team_id)) AS country,
                m.gf AS home_score,
                m.ga AS away_score,
                m.status
            FROM 
                match m
            JOIN 
                team home ON m.team_id = home.team_id
            JOIN 
                team away ON m.opponent_id = away.team_id
            JOIN 
                league l ON home.league_id = l.league_id
            ORDER BY
                m.date DESC
            """
            
            # Execute the query
            results = self.db.execute_query(query)
            
            if not results:
                raise ValueError("No data retrieved from database")
            
            # Convert to DataFrame
            column_names = [
                'id', 'date', 'home_team', 'away_team', 'league', 
                'country', 'home_score', 'away_score', 'status'
            ]
            
            df = pd.DataFrame(results, columns=column_names)
            
            # Make sure date is datetime
            df['date'] = pd.to_datetime(df['date'])
            
            # Add placeholder for start_time if not present
            if 'start_time' not in df.columns:
                df['start_time'] = "00:00"
            
            # Close the connection
            self.db.disconnect()
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading data from database: {str(e)}")
            # Try to disconnect in case the connection is still open
            try:
                if self.db:
                    self.db.disconnect()
            except:
                pass
            
            raise