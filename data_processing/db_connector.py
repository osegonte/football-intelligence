#!/usr/bin/env python3
"""
Database Connector for Football Intelligence Project

This module provides a connection to the existing PostgreSQL database schema
with tables for teams, competitions, seasons, venues, matches, and raw_matches.
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
    Handles connections to the existing PostgreSQL database and provides methods
    for inserting, updating, and querying data.
    """
    
    def __init__(self, config_file='config.ini'):
        """
        Initialize the database connector with configuration from a file.
        
        Args:
            config_file: Path to configuration file
        """
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
        """
        Connect to the PostgreSQL database.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Connect to the database
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
        """
        Execute a SQL query.
        
        Args:
            query: SQL query string
            params: Parameters for the query (tuple or dictionary)
            commit: Whether to commit the transaction
            
        Returns:
            Query results or None if error
        """
        if not self.conn or not self.cur:
            if not self.connect():
                return None
        
        try:
            if params:
                self.cur.execute(query, params)
            else:
                self.cur.execute(query)
            
            # Commit if requested and the query modified data
            if commit and query.strip().upper().startswith(("INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP")):
                self.conn.commit()
            
            # Return results for SELECT queries
            if query.strip().upper().startswith("SELECT"):
                return self.cur.fetchall()
            
            return True
            
        except Exception as e:
            if self.conn:
                self.conn.rollback()
            logger.error(f"Error executing query: {str(e)}")
            logger.error(f"Query: {query}")
            return None
    
    def execute_values(self, query, values, commit=True):
        """
        Execute a query with multiple sets of values.
        
        Args:
            query: SQL query string with %s placeholder for values
            values: List of tuples with values
            commit: Whether to commit the transaction
            
        Returns:
            True if successful, False otherwise
        """
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
            logger.error(f"Query: {query}")
            return False
    
    def get_or_create_team(self, team_name):
        """
        Get or create a team in the database.
        
        Args:
            team_name: Name of the team
            
        Returns:
            Team ID or None if error
        """
        if not self.connect():
            return None
        
        try:
            # Check if team exists
            query = "SELECT id FROM teams WHERE name = %s"
            result = self.execute_query(query, (team_name,))
            
            if result and result[0][0]:
                return result[0][0]
            
            # Team doesn't exist, create it
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
        """
        Get or create a competition in the database.
        
        Args:
            competition_name: Name of the competition
            
        Returns:
            Competition ID or None if error
        """
        if not self.connect():
            return None
        
        try:
            # Check if competition exists
            query = "SELECT id FROM competitions WHERE name = %s"
            result = self.execute_query(query, (competition_name,))
            
            if result and result[0][0]:
                return result[0][0]
            
            # Competition doesn't exist, create it
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
        """
        Get or create a venue in the database.
        
        Args:
            venue_name: Name of the venue
            
        Returns:
            Venue ID or None if error
        """
        # Use "Unknown" for empty venue names
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
            
            # Venue doesn't exist, create it
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
        """
        Get or create a season based on the date.
        
        Args:
            date_str: Date string in format YYYY-MM-DD
            
        Returns:
            Season ID or None if error
        """
        if not date_str:
            date_str = datetime.now().strftime('%Y-%m-%d')
            
        if not self.connect():
            return None
        
        try:
            # Convert date string to datetime
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            
            # Determine season label based on date
            # If month >= 7, season is current year/next year, otherwise previous year/current year
            if date_obj.month >= 7:
                season_label = f"{date_obj.year}/{str(date_obj.year + 1)[2:]}"
            else:
                season_label = f"{date_obj.year - 1}/{str(date_obj.year)[2:]}"
            
            # Check if season exists
            query = "SELECT id FROM seasons WHERE label = %s"
            result = self.execute_query(query, (season_label,))
            
            if result and result[0][0]:
                return result[0][0]
            
            # Season doesn't exist, create it
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
        """
        Store a match in the database.
        
        Args:
            match_data: Dictionary with match data
            
        Returns:
            Match ID or None if error
        """
        if not self.connect():
            return None
        
        try:
            # Get or create related entities
            home_team_id = self.get_or_create_team(match_data.get('home_team'))
            away_team_id = self.get_or_create_team(match_data.get('away_team'))
            competition_id = self.get_or_create_competition(match_data.get('competition', match_data.get('league', 'Unknown')))
            venue_id = self.get_or_create_venue(match_data.get('venue', 'Unknown'))
            season_id = self.get_or_create_season(match_data.get('date'))
            
            if None in [home_team_id, away_team_id, competition_id, venue_id, season_id]:
                logger.error("Failed to get or create related entities")
                return None
            
            # Extract match data with defaults
            home_goals = match_data.get('home_goals', match_data.get('gf', 0))
            away_goals = match_data.get('away_goals', match_data.get('ga', 0))
            home_xg = match_data.get('home_xg', match_data.get('xg', 0))
            away_xg = match_data.get('away_xg', match_data.get('xga', 0))
            home_shots = match_data.get('home_shots', match_data.get('sh', 0))
            away_shots = match_data.get('away_shots', 0)
            home_sot = match_data.get('home_sot', match_data.get('sot', 0))
            away_sot = match_data.get('away_sot', 0)
            home_possession = match_data.get('home_possession', match_data.get('possession', 50))
            away_possession = match_data.get('away_possession', 100 - home_possession)
            home_corners = match_data.get('home_corners', match_data.get('corners', 0))
            away_corners = match_data.get('away_corners', match_data.get('opp_corners', 0))
            
            # Check if match exists based on teams, date, and competition
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
                        home_xg = %s,
                        away_xg = %s,
                        home_shots = %s,
                        away_shots = %s,
                        home_sot = %s,
                        away_sot = %s,
                        home_possession = %s,
                        away_possession = %s,
                        home_corners = %s,
                        away_corners = %s
                    WHERE id = %s
                """
                
                self.execute_query(query, (
                    venue_id,
                    season_id,
                    home_goals,
                    away_goals,
                    home_xg,
                    away_xg,
                    home_shots,
                    away_shots,
                    home_sot,
                    away_sot,
                    home_possession,
                    away_possession,
                    home_corners,
                    away_corners,
                    match_id
                ))
                
                logger.info(f"Updated match: {match_data.get('home_team')} vs {match_data.get('away_team')}")
                return match_id
            
            # Match doesn't exist, create it
            query = """
                INSERT INTO matches (
                    competition_id, season_id, date, venue_id,
                    home_team_id, away_team_id, 
                    home_goals, away_goals, 
                    home_xg, away_xg,
                    home_shots, away_shots,
                    home_sot, away_sot,
                    home_possession, away_possession,
                    home_corners, away_corners
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """
            
            result = self.execute_query(query, (
                competition_id,
                season_id,
                match_data.get('date'),
                venue_id,
                home_team_id,
                away_team_id,
                home_goals,
                away_goals,
                home_xg,
                away_xg,
                home_shots,
                away_shots,
                home_sot,
                away_sot,
                home_possession,
                away_possession,
                home_corners,
                away_corners
            ))
            
            if result and result[0][0]:
                match_id = result[0][0]
                logger.info(f"Created new match: {match_data.get('home_team')} vs {match_data.get('away_team')}")
                return match_id
            
            return None
            
        except Exception as e:
            logger.error(f"Error storing match: {str(e)}")
            return None
        finally:
            self.disconnect()
    
    def store_matches_from_dataframe(self, df):
        """
        Store matches from a pandas DataFrame.
        
        Args:
            df: DataFrame with match data
            
        Returns:
            Number of matches successfully stored
        """
        if df.empty:
            logger.warning("Empty DataFrame, no matches to store")
            return 0
        
        # Check for required columns
        required_columns = ['team', 'opponent', 'date']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.error(f"DataFrame missing required columns: {missing_columns}")
            return 0
        
        # Ensure date is in datetime format
        if not pd.api.types.is_datetime64_dtype(df['date']):
            df['date'] = pd.to_datetime(df['date'])
        
        # Convert each row to a match data dictionary and store
        success_count = 0
        
        for _, row in df.iterrows():
            # Prepare match data
            match_data = {}
            
            # Determine home/away based on venue
            is_home = row.get('venue') == 'Home' or row.get('is_home', False)
            
            if is_home:
                match_data['home_team'] = row['team']
                match_data['away_team'] = row['opponent']
                match_data['home_goals'] = row.get('gf', 0)
                match_data['away_goals'] = row.get('ga', 0)
                match_data['home_xg'] = row.get('xg', 0)
                match_data['away_xg'] = row.get('xga', 0)
                match_data['home_shots'] = row.get('sh', row.get('shots', 0))
                match_data['away_shots'] = 0
                match_data['home_sot'] = row.get('sot', row.get('shots_on_target', 0))
                match_data['away_sot'] = 0
                match_data['home_possession'] = row.get('possession', 50)
                match_data['away_possession'] = 100 - match_data['home_possession']
                match_data['home_corners'] = row.get('corners', 0)
                match_data['away_corners'] = row.get('opp_corners', 0)
            else:
                match_data['home_team'] = row['opponent']
                match_data['away_team'] = row['team']
                match_data['home_goals'] = row.get('ga', 0)
                match_data['away_goals'] = row.get('gf', 0)
                match_data['home_xg'] = row.get('xga', 0)
                match_data['away_xg'] = row.get('xg', 0)
                match_data['home_shots'] = 0
                match_data['away_shots'] = row.get('sh', row.get('shots', 0))
                match_data['home_sot'] = 0
                match_data['away_sot'] = row.get('sot', row.get('shots_on_target', 0))
                match_data['home_possession'] = 100 - row.get('possession', 50)
                match_data['away_possession'] = row.get('possession', 50)
                match_data['home_corners'] = row.get('opp_corners', 0)
                match_data['away_corners'] = row.get('corners', 0)
            
            # Add common fields
            match_data['date'] = row['date'].strftime('%Y-%m-%d')
            match_data['competition'] = row.get('competition', row.get('league', 'Unknown'))
            match_data['venue'] = row.get('venue', 'Unknown')
            
            # Store match
            match_id = self.store_match(match_data)
            
            if match_id:
                success_count += 1
        
        return success_count
    
    def store_matches_from_csv(self, csv_file):
        """
        Store matches from a CSV file.
        
        Args:
            csv_file: Path to CSV file
            
        Returns:
            Number of matches successfully stored
        """
        try:
            # Read CSV file
            df = pd.read_csv(csv_file)
            
            # Store matches from DataFrame
            return self.store_matches_from_dataframe(df)
            
        except Exception as e:
            logger.error(f"Error reading CSV file: {str(e)}")
            return 0
    
    def get_team_matches(self, team_name, limit=10):
        """
        Get matches for a team from the team_match_stats view.
        
        Args:
            team_name: Name of the team
            limit: Maximum number of matches to return
            
        Returns:
            List of dictionaries with match data
        """
        if not self.connect():
            return []
        
        try:
            # Query team matches from the view
            query = """
                SELECT * FROM team_match_stats
                WHERE team = %s
                ORDER BY date DESC
                LIMIT %s
            """
            
            result = self.execute_query(query, (team_name, limit))
            
            if not result:
                return []
            
            # Get column names
            column_names = [desc[0] for desc in self.cur.description]
            
            # Convert to list of dictionaries
            matches = []
            for row in result:
                match_dict = {column_names[i]: value for i, value in enumerate(row)}
                matches.append(match_dict)
            
            return matches
            
        except Exception as e:
            logger.error(f"Error getting team matches: {str(e)}")
            return []
        finally:
            self.disconnect()
    
    def get_upcoming_fixtures(self, team_name=None, competition_name=None, days=7):
        """
        Get upcoming fixtures from the raw_matches table.
        
        Args:
            team_name: Optional team name to filter by
            competition_name: Optional competition name to filter by
            days: Number of days to look ahead
            
        Returns:
            List of dictionaries with upcoming fixtures
        """
        if not self.connect():
            return []
        
        try:
            # Build query based on filters
            query = """
                SELECT id, date, home_team, away_team, league AS competition, venue, round
                FROM raw_matches
                WHERE date >= CURRENT_DATE AND date <= CURRENT_DATE + %s
            """
            
            params = [days]
            
            if team_name:
                query += " AND (home_team = %s OR away_team = %s)"
                params.extend([team_name, team_name])
            
            if competition_name:
                query += " AND league = %s"
                params.append(competition_name)
            
            query += " ORDER BY date ASC"
            
            result = self.execute_query(query, params)
            
            if not result:
                return []
            
            # Get column names
            column_names = [desc[0] for desc in self.cur.description]
            
            # Convert to list of dictionaries
            fixtures = []
            for row in result:
                fixture_dict = {column_names[i]: value for i, value in enumerate(row)}
                fixtures.append(fixture_dict)
            
            return fixtures
            
        except Exception as e:
            logger.error(f"Error getting upcoming fixtures: {str(e)}")
            return []
        finally:
            self.disconnect()
    
    def get_team_stats(self, team_name):
        """
        Get statistics for a team from the matches table.
        
        Args:
            team_name: Name of the team
            
        Returns:
            Dictionary with team statistics
        """
        if not self.connect():
            return {}
        
        try:
            # Get team ID
            team_query = "SELECT id FROM teams WHERE name = %s"
            team_result = self.execute_query(team_query, (team_name,))
            
            if not team_result or not team_result[0][0]:
                logger.error(f"Team not found: {team_name}")
                return {}
            
            team_id = team_result[0][0]
            
            # Query team statistics
            query = """
                WITH home_matches AS (
                    SELECT 
                        CASE 
                            WHEN home_goals > away_goals THEN 'W'
                            WHEN home_goals = away_goals THEN 'D'
                            ELSE 'L'
                        END AS result,
                        home_goals AS gf,
                        away_goals AS ga,
                        home_xg AS xg,
                        away_xg AS xga,
                        home_shots AS sh,
                        away_shots AS opp_sh,
                        home_sot AS sot,
                        away_sot AS opp_sot,
                        home_possession AS possession,
                        home_corners AS corners,
                        away_corners AS opp_corners
                    FROM matches
                    WHERE home_team_id = %s
                ),
                away_matches AS (
                    SELECT 
                        CASE 
                            WHEN away_goals > home_goals THEN 'W'
                            WHEN away_goals = home_goals THEN 'D'
                            ELSE 'L'
                        END AS result,
                        away_goals AS gf,
                        home_goals AS ga,
                        away_xg AS xg,
                        home_xg AS xga,
                        away_shots AS sh,
                        home_shots AS opp_sh,
                        away_sot AS sot,
                        home_sot AS opp_sot,
                        away_possession AS possession,
                        away_corners AS corners,
                        home_corners AS opp_corners
                    FROM matches
                    WHERE away_team_id = %s
                ),
                all_matches AS (
                    SELECT * FROM home_matches
                    UNION ALL
                    SELECT * FROM away_matches
                )
                SELECT
                    COUNT(*) AS matches_played,
                    COUNT(*) FILTER (WHERE result = 'W') AS wins,
                    COUNT(*) FILTER (WHERE result = 'D') AS draws,
                    COUNT(*) FILTER (WHERE result = 'L') AS losses,
                    SUM(gf) AS goals_for,
                    SUM(ga) AS goals_against,
                    SUM(gf) - SUM(ga) AS goal_diff,
                    ROUND(AVG(xg), 2) AS avg_xg,
                    ROUND(AVG(xga), 2) AS avg_xga,
                    ROUND(AVG(possession), 2) AS avg_possession,
                    ROUND(AVG(sh), 2) AS avg_shots,
                    ROUND(AVG(sot), 2) AS avg_shots_on_target,
                    ROUND(AVG(corners), 2) AS avg_corners
                FROM all_matches
            """
            
            result = self.execute_query(query, (team_id, team_id))
            
            if not result or not result[0]:
                logger.warning(f"No match statistics found for team: {team_name}")
                return {}
            
            # Get column names
            column_names = [desc[0] for desc in self.cur.description]
            
            # Convert to dictionary
            stats = {column_names[i]: value for i, value in enumerate(result[0])}
            
            # Add team name and points
            stats['team_name'] = team_name
            stats['points'] = (stats.get('wins', 0) * 3) + stats.get('draws', 0)
            
            # Calculate points per game
            if stats.get('matches_played', 0) > 0:
                stats['points_per_game'] = round(stats['points'] / stats['matches_played'], 2)
            else:
                stats['points_per_game'] = 0.0
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting team statistics: {str(e)}")
            return {}
        finally:
            self.disconnect()
    
    def get_competition_table(self, competition_name):
        """
        Generate a league table for a competition.
        
        Args:
            competition_name: Name of the competition
            
        Returns:
            List of dictionaries with team standings
        """
        if not self.connect():
            return []
        
        try:
            # Get competition ID
            comp_query = "SELECT id FROM competitions WHERE name = %s"
            comp_result = self.execute_query(comp_query, (competition_name,))
            
            if not comp_result or not comp_result[0][0]:
                logger.error(f"Competition not found: {competition_name}")
                return []
            
            competition_id = comp_result[0][0]
            
            # Get current season ID (most recent)
            season_query = """
                SELECT id FROM seasons
                ORDER BY label DESC
                LIMIT 1
            """
            
            season_result = self.execute_query(season_query)
            
            if not season_result or not season_result[0][0]:
                logger.error("No seasons found")
                return []
            
            season_id = season_result[0][0]
            
            # Query league table
            query = """
                WITH team_results AS (
                    -- Home matches
                    SELECT
                        home_team_id AS team_id,
                        CASE 
                            WHEN home_goals > away_goals THEN 3
                            WHEN home_goals = away_goals THEN 1
                            ELSE 0
                        END AS points,
                        CASE 
                            WHEN home_goals > away_goals THEN 1
                            ELSE 0
                        END AS wins,
                        CASE 
                            WHEN home_goals = away_goals THEN 1
                            ELSE 0
                        END AS draws,
                        CASE 
                            WHEN home_goals < away_goals THEN 1
                            ELSE 0
                        END AS losses,
                        home_goals AS goals_for,
                        away_goals AS goals_against,
                        home_goals - away_goals AS goal_diff
                    FROM matches
                    WHERE competition_id = %s
                    AND season_id = %s
                    
                    UNION ALL
                    
                    -- Away matches
                    SELECT
                        away_team_id AS team_id,
                        CASE 
                            WHEN away_goals > home_goals THEN 3
                            WHEN away_goals = home_goals THEN 1
                            ELSE 0
                        END AS points,
                        CASE 
                            WHEN away_goals > home_goals THEN 1
                            ELSE 0
                        END AS wins,
                        CASE 
                            WHEN away_goals = home_goals THEN 1
                            ELSE 0
                        END AS draws,
                        CASE 
                            WHEN away_goals < home_goals THEN 1
                            ELSE 0
                        END AS losses,
                        away_goals AS goals_for,
                        home_goals AS goals_against,
                        away_goals - home_goals AS goal_diff
                    FROM matches
                    WHERE competition_id = %s
                    AND season_id = %s
                ),
                team_standings AS (
                    SELECT
                        team_id,
                        SUM(points) AS points,
                        SUM(wins) AS wins,
                        SUM(draws) AS draws,
                        SUM(losses) AS losses,
                        SUM(wins) + SUM(draws) + SUM(losses) AS matches_played,
                        SUM(goals_for) AS goals_for,
                        SUM(goals_against) AS goals_against,
                        SUM(goal_diff) AS goal_diff
                    FROM team_results
                    GROUP BY team_id
                )
                SELECT
                    t.name AS team_name,
                    ts.matches_played,
                    ts.wins,
                    ts.draws,
                    ts.losses,
                    ts.points,
                    ts.goals_for,
                    ts.goals_against,
                    ts.goal_diff
                FROM team_standings ts
                JOIN teams t ON ts.team_id = t.id
                ORDER BY 
                    ts.points DESC, 
                    ts.goal_diff DESC, 
                    ts.goals_for DESC,
                    t.name ASC
            """
            
            result = self.execute_query(query, (competition_id, season_id, competition_id, season_id))
            
            if not result:
                logger.warning(f"No table data found for competition: {competition_name}")
                return []
            
            # Get column names
            column_names = [desc[0] for desc in self.cur.description]
            
            # Convert to list of dictionaries
            table = []
            for i, row in enumerate(result):
                team_dict = {column_names[j]: value for j, value in enumerate(row)}
                team_dict['position'] = i + 1  # Add position
                table.append(team_dict)
            
            return table
            
        except Exception as e:
            logger.error(f"Error getting competition table: {str(e)}")
            return []
        finally:
            self.disconnect()
    
    def get_head_to_head(self, team1_name, team2_name, limit=10):
        """
        Get head-to-head match history between two teams.
        
        Args:
            team1_name: First team name
            team2_name: Second team name
            limit: Maximum number of matches to return
            
        Returns:
            List of dictionaries with match data
        """
        if not self.connect():
            return []
        
        try:
            # Get team IDs
            team1_query = "SELECT id FROM teams WHERE name = %s"
            team1_result = self.execute_query(team1_query, (team1_name,))
            
            if not team1_result or not team1_result[0][0]:
                logger.error(f"Team not found: {team1_name}")
                return []
            
            team1_id = team1_result[0][0]
            
            team2_query = "SELECT id FROM teams WHERE name = %s"
            team2_result = self.execute_query(team2_query, (team2_name,))
            
            if not team2_result or not team2_result[0][0]:
                logger.error(f"Team not found: {team2_name}")
                return []
            
            team2_id = team2_result[0][0]
            
            # Query head-to-head matches
            query = """
                SELECT 
                    m.id,
                    m.date,
                    c.name AS competition,
                    s.label AS season,
                    ht.name AS home_team,
                    at.name AS away_team,
                    m.home_goals,
                    m.away_goals,
                    m.home_xg,
                    m.away_xg,
                    m.home_shots,
                    m.away_shots,
                    m.home_sot,
                    m.away_sot,
                    m.home_possession,
                    m.away_possession,
                    m.home_corners,
                    m.away_corners,
                    v.name AS venue
                FROM matches m
                JOIN teams ht ON m.home_team_id = ht.id
                JOIN teams at ON m.away_team_id = at.id
                JOIN competitions c ON m.competition_id = c.id
                JOIN seasons s ON m.season_id = s.id
                JOIN venues v ON m.venue_id = v.id
                WHERE ((m.home_team_id = %s AND m.away_team_id = %s)
                    OR (m.home_team_id = %s AND m.away_team_id = %s))
                    AND m.date <= CURRENT_DATE
                ORDER BY m.date DESC
                LIMIT %s
            """
            
            result = self.execute_query(query, (team1_id, team2_id, team2_id, team1_id, limit))
            
            if not result:
                logger.info(f"No head-to-head matches found between {team1_name} and {team2_name}")
                return []
            
            # Get column names
            column_names = [desc[0] for desc in self.cur.description]
            
            # Convert to list of dictionaries
            matches = []
            for row in result:
                match_dict = {column_names[i]: value for i, value in enumerate(row)}
                
                # Add result information
                if match_dict['home_team'] == team1_name:
                    if match_dict['home_goals'] > match_dict['away_goals']:
                        match_dict['result'] = f"{team1_name} win"
                    elif match_dict['home_goals'] < match_dict['away_goals']:
                        match_dict['result'] = f"{team2_name} win"
                    else:
                        match_dict['result'] = "Draw"
                else:
                    if match_dict['home_goals'] > match_dict['away_goals']:
                        match_dict['result'] = f"{team2_name} win"
                    elif match_dict['home_goals'] < match_dict['away_goals']:
                        match_dict['result'] = f"{team1_name} win"
                    else:
                        match_dict['result'] = "Draw"
                
                matches.append(match_dict)
            
            return matches
            
        except Exception as e:
            logger.error(f"Error getting head-to-head matches: {str(e)}")
            return []
        finally:
            self.disconnect()
    
    def get_database_statistics(self):
        """
        Get general statistics about the database.
        
        Returns:
            Dictionary with database statistics
        """
        if not self.connect():
            return {}
        
        try:
            stats = {}
            
            # Count teams
            team_query = "SELECT COUNT(*) FROM teams"
            team_result = self.execute_query(team_query)
            if team_result and team_result[0][0]:
                stats['team_count'] = team_result[0][0]
            
            # Count competitions
            comp_query = "SELECT COUNT(*) FROM competitions"
            comp_result = self.execute_query(comp_query)
            if comp_result and comp_result[0][0]:
                stats['competition_count'] = comp_result[0][0]
            
            # Count matches
            match_query = "SELECT COUNT(*) FROM matches"
            match_result = self.execute_query(match_query)
            if match_result and match_result[0][0]:
                stats['match_count'] = match_result[0][0]
            
            # Get date range
            date_query = "SELECT MIN(date), MAX(date) FROM matches"
            date_result = self.execute_query(date_query)
            if date_result and date_result[0]:
                stats['first_match_date'] = date_result[0][0]
                stats['last_match_date'] = date_result[0][1]
            
            # Count seasons
            season_query = "SELECT COUNT(*) FROM seasons"
            season_result = self.execute_query(season_query)
            if season_result and season_result[0][0]:
                stats['season_count'] = season_result[0][0]
            
            # Top competitions by match count
            top_comp_query = """
                SELECT c.name, COUNT(*) AS match_count
                FROM matches m
                JOIN competitions c ON m.competition_id = c.id
                GROUP BY c.name
                ORDER BY match_count DESC
                LIMIT 5
            """
            
            top_comp_result = self.execute_query(top_comp_query)
            if top_comp_result:
                stats['top_competitions'] = [{'name': row[0], 'match_count': row[1]} for row in top_comp_result]
            
            # Top teams by match count
            top_team_query = """
                WITH team_matches AS (
                    SELECT home_team_id AS team_id FROM matches
                    UNION ALL
                    SELECT away_team_id AS team_id FROM matches
                )
                SELECT t.name, COUNT(*) AS match_count
                FROM team_matches tm
                JOIN teams t ON tm.team_id = t.id
                GROUP BY t.name
                ORDER BY match_count DESC
                LIMIT 10
            """
            
            top_team_result = self.execute_query(top_team_query)
            if top_team_result:
                stats['top_teams'] = [{'name': row[0], 'match_count': row[1]} for row in top_team_result]
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting database statistics: {str(e)}")
            return {}
        finally:
            self.disconnect()
    
    def test_connection(self):
        """Test the database connection."""
        if self.connect():
            self.disconnect()
            return True
        return False
    
    def get_match_by_teams_and_date(self, home_team, away_team, match_date):
        """
        Get a match by home team, away team, and date.
        
        Args:
            home_team: Home team name
            away_team: Away team name
            match_date: Match date (string in format YYYY-MM-DD)
            
        Returns:
            Dictionary with match data or None if not found
        """
        if not self.connect():
            return None
        
        try:
            # Get team IDs
            home_team_query = "SELECT id FROM teams WHERE name = %s"
            home_team_result = self.execute_query(home_team_query, (home_team,))
            
            if not home_team_result or not home_team_result[0][0]:
                logger.error(f"Home team not found: {home_team}")
                return None
            
            home_team_id = home_team_result[0][0]
            
            away_team_query = "SELECT id FROM teams WHERE name = %s"
            away_team_result = self.execute_query(away_team_query, (away_team,))
            
            if not away_team_result or not away_team_result[0][0]:
                logger.error(f"Away team not found: {away_team}")
                return None
            
            away_team_id = away_team_result[0][0]
            
            # Query the match
            query = """
                SELECT 
                    m.id,
                    m.date,
                    c.name AS competition,
                    s.label AS season,
                    ht.name AS home_team,
                    at.name AS away_team,
                    m.home_goals,
                    m.away_goals,
                    m.home_xg,
                    m.away_xg,
                    m.home_shots,
                    m.away_shots,
                    m.home_sot,
                    m.away_sot,
                    m.home_possession,
                    m.away_possession,
                    m.home_corners,
                    m.away_corners,
                    v.name AS venue
                FROM matches m
                JOIN teams ht ON m.home_team_id = ht.id
                JOIN teams at ON m.away_team_id = at.id
                JOIN competitions c ON m.competition_id = c.id
                JOIN seasons s ON m.season_id = s.id
                JOIN venues v ON m.venue_id = v.id
                WHERE m.home_team_id = %s
                    AND m.away_team_id = %s
                    AND m.date = %s
            """
            
            result = self.execute_query(query, (home_team_id, away_team_id, match_date))
            
            if not result or not result[0]:
                logger.info(f"No match found for {home_team} vs {away_team} on {match_date}")
                return None
            
            # Get column names
            column_names = [desc[0] for desc in self.cur.description]
            
            # Convert to dictionary
            match_dict = {column_names[i]: value for i, value in enumerate(result[0])}
            
            return match_dict
            
        except Exception as e:
            logger.error(f"Error getting match: {str(e)}")
            return None
        finally:
            self.disconnect()