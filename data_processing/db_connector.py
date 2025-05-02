"""
PostgreSQL database connector for storing FBref match statistics.
"""
import os
import logging
import pandas as pd
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values

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

class DatabaseConnector:
    """
    PostgreSQL database connector for the Football Intelligence project.
    Handles database connections, schema creation, and match data operations.
    """
    
    def __init__(self, 
                 dbname="fbref_stats", 
                 user=None, 
                 password=None, 
                 host="localhost", 
                 port=5432,
                 use_environment=True):
        """
        Initialize the database connector.
        
        Args:
            dbname: Database name
            user: Database user (if None and use_environment is True, will try to get from env var)
            password: Database password (if None and use_environment is True, will try to get from env var)
            host: Database host
            port: Database port
            use_environment: Whether to try getting credentials from environment variables
        """
        self.dbname = dbname
        self.host = host
        self.port = port
        
        # Try to get credentials from environment variables if enabled
        if use_environment:
            self.user = user or os.environ.get("DB_USER", "")
            self.password = password or os.environ.get("DB_PASSWORD", "")
        else:
            self.user = user
            self.password = password
        
        # Connection and cursor will be set when connecting
        self.conn = None
        self.cur = None
    
    def connect(self):
        """
        Connect to the PostgreSQL database.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Build connection string
            conn_params = {
                "dbname": self.dbname,
                "host": self.host,
                "port": self.port
            }
            
            # Add user and password if provided
            if self.user:
                conn_params["user"] = self.user
            if self.password:
                conn_params["password"] = self.password
            
            # Connect to the database
            self.conn = psycopg2.connect(**conn_params)
            self.cur = self.conn.cursor()
            
            logger.info(f"Connected to PostgreSQL database: {self.dbname}")
            return True
            
        except Exception as e:
            logger.error(f"Error connecting to database: {str(e)}")
            return False
    
    def disconnect(self):
        """Close the database connection."""
        if self.cur:
            self.cur.close()
        
        if self.conn:
            self.conn.close()
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
            self.conn.rollback()
            logger.error(f"Error executing values: {str(e)}")
            logger.error(f"Query: {query}")
            return False
    
    def create_schema(self):
        """
        Create the database schema if it doesn't exist.
        Designed for FBref match statistics data.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create league table
            self.execute_query("""
                CREATE TABLE IF NOT EXISTS league (
                    league_id   SERIAL PRIMARY KEY,
                    league_name TEXT   NOT NULL UNIQUE,
                    country     TEXT
                )
            """)
            
            # Create team table
            self.execute_query("""
                CREATE TABLE IF NOT EXISTS team (
                    team_id     SERIAL PRIMARY KEY,
                    team_name   TEXT   NOT NULL UNIQUE,
                    league_id   INTEGER 
                        REFERENCES league(league_id)
                        ON UPDATE CASCADE
                        ON DELETE RESTRICT
                )
            """)
            
            # Create match table with fields for FBref match stats
            self.execute_query("""
                CREATE TABLE IF NOT EXISTS match (
                    match_id        TEXT      PRIMARY KEY,
                    date            DATE      NOT NULL,
                    team_id         INTEGER   NOT NULL
                        REFERENCES team(team_id),
                    opponent_id     INTEGER   NOT NULL
                        REFERENCES team(team_id),
                    venue           TEXT,
                    competition     TEXT,
                    round           TEXT,
                    result          TEXT,
                    gf              SMALLINT,
                    ga              SMALLINT,
                    xg              REAL,
                    xga             REAL,
                    sh              SMALLINT,
                    sot             SMALLINT,
                    dist            REAL,
                    fk              SMALLINT,
                    pk              SMALLINT,
                    pkatt           SMALLINT,
                    possession      REAL,
                    yellow_cards    SMALLINT,
                    red_cards       SMALLINT,
                    fouls           SMALLINT,
                    corners         SMALLINT,
                    opp_corners     SMALLINT,
                    scrape_date     TIMESTAMP NOT NULL DEFAULT now(),
                    source          TEXT
                )
            """)
            
            # Create index for quick lookups
            self.execute_query("""
                CREATE INDEX IF NOT EXISTS idx_match_team_date
                    ON match(team_id, date DESC)
            """)
            
            logger.info("Database schema created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error creating schema: {str(e)}")
            return False
    
    def import_leagues_from_csv(self, csv_path):
        """
        Import leagues from a CSV file.
        
        Args:
            csv_path: Path to the CSV file
            
        Returns:
            Number of leagues imported
        """
        try:
            # Read CSV file
            df = pd.read_csv(csv_path)
            
            # Check for league/competition column
            league_col = None
            for col in ['league', 'competition', 'comp']:
                if col in df.columns:
                    league_col = col
                    break
            
            if not league_col:
                logger.error("CSV file does not contain a league/competition column")
                return 0
            
            # Get unique leagues
            leagues = df[league_col].dropna().unique()
            
            # Insert leagues into database
            count = 0
            for league in leagues:
                # Try to determine country from data
                country = "Unknown"
                if 'country' in df.columns:
                    country_values = df[df[league_col] == league]['country'].dropna().unique()
                    if len(country_values) > 0:
                        country = country_values[0]
                
                result = self.execute_query(
                    "INSERT INTO league (league_name, country) VALUES (%s, %s) ON CONFLICT (league_name) DO NOTHING",
                    (league, country)
                )
                if result:
                    count += 1
            
            logger.info(f"Imported {count} leagues from {csv_path}")
            return count
            
        except Exception as e:
            logger.error(f"Error importing leagues from CSV: {str(e)}")
            return 0
    
    def import_teams_from_csv(self, csv_path):
        """
        Import teams from a CSV file.
        
        Args:
            csv_path: Path to the CSV file
            
        Returns:
            Number of teams imported
        """
        try:
            # Read CSV file
            df = pd.read_csv(csv_path)
            
            # Check for required columns
            required_cols = ['team', 'opponent']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                logger.error(f"CSV file missing required columns: {missing_cols}")
                return 0
            
            # Find league column
            league_col = None
            for col in ['league', 'competition', 'comp']:
                if col in df.columns:
                    league_col = col
                    break
            
            # Get all unique teams
            teams = set(df['team'].dropna().unique()) | set(df['opponent'].dropna().unique())
            
            # Insert teams into database
            count = 0
            for team in teams:
                # Try to find the team's league
                league_id = None
                if league_col:
                    team_leagues = df[df['team'] == team][league_col].dropna().unique()
                    
                    if len(team_leagues) > 0:
                        # Get the league ID
                        self.cur.execute(
                            "SELECT league_id FROM league WHERE league_name = %s",
                            (team_leagues[0],)
                        )
                        league_result = self.cur.fetchone()
                        
                        if league_result:
                            league_id = league_result[0]
                
                # Insert the team
                if league_id:
                    query = """
                    INSERT INTO team (team_name, league_id) 
                    VALUES (%s, %s) 
                    ON CONFLICT (team_name) DO NOTHING
                    """
                    result = self.execute_query(query, (team, league_id))
                else:
                    query = """
                    INSERT INTO team (team_name) 
                    VALUES (%s) 
                    ON CONFLICT (team_name) DO NOTHING
                    """
                    result = self.execute_query(query, (team,))
                
                if result:
                    count += 1
            
            logger.info(f"Imported {count} teams from {csv_path}")
            return count
            
        except Exception as e:
            logger.error(f"Error importing teams from CSV: {str(e)}")
            return 0
    
    def import_matches_from_csv(self, csv_path):
        """
        Import matches from a CSV file.
        
        Args:
            csv_path: Path to the CSV file
            
        Returns:
            Number of matches imported
        """
        try:
            # Read CSV file
            df = pd.read_csv(csv_path)
            
            # Check for match_id and date columns
            if 'match_id' not in df.columns:
                # Create match_id if it doesn't exist
                if all(col in df.columns for col in ['date', 'team', 'opponent']):
                    df['match_id'] = df.apply(
                        lambda row: f"{str(row['date'])}_{row['team']}_{row['opponent']}",
                        axis=1
                    )
                else:
                    logger.error("CSV file missing required columns to create match_id")
                    return 0
            
            if 'date' not in df.columns:
                logger.error("CSV file missing 'date' column")
                return 0
            
            # Ensure date is in datetime format
            df['date'] = pd.to_datetime(df['date'])
            
            # Prepare matches for insertion
            match_data = []
            
            for _, row in df.iterrows():
                # Get team IDs
                team_name = row['team']
                opponent_name = row['opponent']
                
                self.cur.execute("SELECT team_id FROM team WHERE team_name = %s", (team_name,))
                team_result = self.cur.fetchone()
                
                self.cur.execute("SELECT team_id FROM team WHERE team_name = %s", (opponent_name,))
                opponent_result = self.cur.fetchone()
                
                if not team_result or not opponent_result:
                    logger.warning(f"Team not found: {team_name} or {opponent_name}")
                    continue
                
                team_id = team_result[0]
                opponent_id = opponent_result[0]
                
                # Extract match data
                match_data.append((
                    row['match_id'],
                    row['date'].strftime('%Y-%m-%d'),
                    team_id,
                    opponent_id,
                    row.get('venue', None),
                    row.get('competition', row.get('league', None)),
                    row.get('round', None),
                    row.get('result', None),
                    row.get('gf', None),
                    row.get('ga', None),
                    row.get('xg', None),
                    row.get('xga', None),
                    row.get('sh', None),
                    row.get('sot', None),
                    row.get('dist', None),
                    row.get('fk', None),
                    row.get('pk', None),
                    row.get('pkatt', None),
                    row.get('possession', None),
                    row.get('yellow_cards', None),
                    row.get('red_cards', None),
                    row.get('fouls', None),
                    row.get('corners', None),
                    row.get('opp_corners', None),
                    datetime.now(),
                    row.get('source', 'fbref')
                ))
            
            # Batch insert matches
            if match_data:
                insert_query = """
                INSERT INTO match(
                    match_id, date, team_id, opponent_id, venue, competition,
                    round, result, gf, ga, xg, xga, sh, sot, dist, fk, pk, pkatt,
                    possession, yellow_cards, red_cards, fouls, corners, opp_corners,
                    scrape_date, source
                )
                VALUES %s
                ON CONFLICT(match_id) DO UPDATE
                SET
                    venue        = EXCLUDED.venue,
                    competition  = EXCLUDED.competition,
                    round        = EXCLUDED.round,
                    result       = EXCLUDED.result,
                    gf           = EXCLUDED.gf,
                    ga           = EXCLUDED.ga,
                    xg           = EXCLUDED.xg,
                    xga          = EXCLUDED.xga,
                    sh           = EXCLUDED.sh,
                    sot          = EXCLUDED.sot,
                    dist         = EXCLUDED.dist,
                    fk           = EXCLUDED.fk,
                    pk           = EXCLUDED.pk,
                    pkatt        = EXCLUDED.pkatt,
                    possession   = EXCLUDED.possession,
                    yellow_cards = EXCLUDED.yellow_cards,
                    red_cards    = EXCLUDED.red_cards,
                    fouls        = EXCLUDED.fouls,
                    corners      = EXCLUDED.corners,
                    opp_corners  = EXCLUDED.opp_corners,
                    scrape_date  = EXCLUDED.scrape_date,
                    source       = EXCLUDED.source
                """
                
                success = self.execute_values(insert_query, match_data)
                
                if success:
                    logger.info(f"Imported {len(match_data)} matches from {csv_path}")
                    return len(match_data)
            
            return 0
            
        except Exception as e:
            logger.error(f"Error importing matches from CSV: {str(e)}")
            return 0
    
    def test_connection(self):
        """
        Test the database connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        if self.connect():
            self.disconnect()
            return True
        return False