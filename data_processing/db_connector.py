"""
PostgreSQL database connector for the Football Intelligence project.
Focuses solely on storing raw data without any processing or analysis.
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
    Handles database connections, schema creation, and raw data operations.
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
        Focuses on storing raw data without any processing.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create league table
            self.execute_query("""
                CREATE TABLE IF NOT EXISTS league (
                    league_id   SERIAL PRIMARY KEY,
                    league_name TEXT   NOT NULL UNIQUE
                )
            """)
            
            # Create team table
            self.execute_query("""
                CREATE TABLE IF NOT EXISTS team (
                    team_id     SERIAL PRIMARY KEY,
                    team_name   TEXT   NOT NULL UNIQUE,
                    league_id   INTEGER NOT NULL 
                        REFERENCES league(league_id)
                        ON UPDATE CASCADE
                        ON DELETE RESTRICT
                )
            """)
            
            # Create match table with all raw data fields
            self.execute_query("""
                CREATE TABLE IF NOT EXISTS match (
                    match_id    TEXT      PRIMARY KEY,
                    date        DATE      NOT NULL,
                    team_id     INTEGER   NOT NULL
                        REFERENCES team(team_id),
                    opponent_id INTEGER   NOT NULL
                        REFERENCES team(team_id),
                    gf          SMALLINT,
                    ga          SMALLINT,
                    sh          SMALLINT,
                    sot         SMALLINT,
                    dist        REAL,
                    fk          SMALLINT,
                    pk          SMALLINT,
                    pkatt       SMALLINT,
                    corners     SMALLINT,
                    opp_corners SMALLINT,
                    scrape_date TIMESTAMP NOT NULL DEFAULT now(),
                    source      TEXT,
                    status      TEXT      NOT NULL
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
        Import leagues from a CSV file without any processing.
        
        Args:
            csv_path: Path to the CSV file
            
        Returns:
            Number of leagues imported
        """
        try:
            # Read CSV file
            df = pd.read_csv(csv_path)
            
            if 'league' not in df.columns:
                logger.error("CSV file does not contain a 'league' column")
                return 0
            
            # Get unique leagues
            leagues = df['league'].dropna().unique()
            
            # Insert leagues into database
            count = 0
            for league in leagues:
                result = self.execute_query(
                    "INSERT INTO league (league_name) VALUES (%s) ON CONFLICT (league_name) DO NOTHING",
                    (league,)
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
        Import teams from a CSV file without any processing.
        
        Args:
            csv_path: Path to the CSV file
            
        Returns:
            Number of teams imported
        """
        try:
            # Read CSV file
            df = pd.read_csv(csv_path)
            
            if 'home_team' not in df.columns or 'away_team' not in df.columns or 'league' not in df.columns:
                logger.error("CSV file does not contain required columns: home_team, away_team, league")
                return 0
            
            # Get all teams and their leagues
            home_teams = df[['home_team', 'league']].drop_duplicates().dropna()
            away_teams = df[['away_team', 'league']].drop_duplicates().dropna()
            away_teams.columns = ['home_team', 'league']  # Rename for concatenation
            
            all_teams = pd.concat([home_teams, away_teams]).drop_duplicates()
            
            # Insert teams into database
            count = 0
            for _, row in all_teams.iterrows():
                # First, make sure the league exists
                self.cur.execute(
                    "SELECT league_id FROM league WHERE league_name = %s",
                    (row['league'],)
                )
                result = self.cur.fetchone()
                
                if result:
                    league_id = result[0]
                    
                    # Insert the team if it doesn't exist
                    result = self.execute_query(
                        """
                        INSERT INTO team (team_name, league_id) 
                        VALUES (%s, %s) 
                        ON CONFLICT (team_name) DO NOTHING
                        """,
                        (row['home_team'], league_id)
                    )
                    
                    if result:
                        count += 1
            
            logger.info(f"Imported {count} teams from {csv_path}")
            return count
            
        except Exception as e:
            logger.error(f"Error importing teams from CSV: {str(e)}")
            return 0
    
    def import_matches_from_csv(self, csv_path):
        """
        Import matches from a CSV file without any processing.
        
        Args:
            csv_path: Path to the CSV file
            
        Returns:
            Number of matches imported
        """
        try:
            # Read CSV file
            df = pd.read_csv(csv_path)
            
            required_columns = ['id', 'home_team', 'away_team', 'date']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                logger.error(f"CSV file missing required columns: {missing_columns}")
                return 0
            
            # Convert date column to datetime if it's not already
            if df['date'].dtype == 'object':
                df['date'] = pd.to_datetime(df['date'])
            
            # Prepare a list to hold match data for batch insert
            match_data = []
            
            for _, row in df.iterrows():
                # Get team IDs from database
                self.cur.execute("SELECT team_id FROM team WHERE team_name = %s", (row['home_team'],))
                home_result = self.cur.fetchone()
                
                self.cur.execute("SELECT team_id FROM team WHERE team_name = %s", (row['away_team'],))
                away_result = self.cur.fetchone()
                
                if not home_result or not away_result:
                    logger.warning(f"Team not found in database: {row['home_team']} or {row['away_team']}")
                    continue
                
                team_id = home_result[0]
                opponent_id = away_result[0]
                
                # Extract match stats if available - storing raw values without processing
                match_stats = {
                    'gf': row.get('home_score', None),
                    'ga': row.get('away_score', None),
                    'sh': row.get('home_shots', None),
                    'sot': row.get('home_shots_on_target', None),
                    'dist': row.get('home_shot_distance', None),
                    'fk': row.get('home_free_kicks', None),
                    'pk': row.get('home_penalties', None),
                    'pkatt': row.get('home_penalty_attempts', None),
                    'corners': row.get('home_corners', None),
                    'opp_corners': row.get('away_corners', None),
                    'source': row.get('source', None),
                }
                
                # Determine match status
                status = row.get('status', 'scheduled')
                if pd.isna(status) or status == '':
                    status = 'scheduled'
                
                # Add match to batch
                match_data.append((
                    row['id'],
                    row['date'].strftime('%Y-%m-%d'),
                    team_id,
                    opponent_id,
                    match_stats['gf'],
                    match_stats['ga'],
                    match_stats['sh'],
                    match_stats['sot'],
                    match_stats['dist'],
                    match_stats['fk'],
                    match_stats['pk'],
                    match_stats['pkatt'],
                    match_stats['corners'],
                    match_stats['opp_corners'],
                    datetime.now(),
                    match_stats['source'],
                    status
                ))
            
            # Batch insert using execute_values
            if match_data:
                insert_query = """
                INSERT INTO match(
                    match_id, date, team_id, opponent_id,
                    gf, ga, sh, sot, dist, fk, pk, pkatt, 
                    corners, opp_corners, scrape_date, source, status
                )
                VALUES %s
                ON CONFLICT(match_id) DO UPDATE
                SET
                    gf          = EXCLUDED.gf,
                    ga          = EXCLUDED.ga,
                    sh          = EXCLUDED.sh,
                    sot         = EXCLUDED.sot,
                    dist        = EXCLUDED.dist,
                    fk          = EXCLUDED.fk,
                    pk          = EXCLUDED.pk,
                    pkatt       = EXCLUDED.pkatt,
                    corners     = EXCLUDED.corners,
                    opp_corners = EXCLUDED.opp_corners,
                    source      = EXCLUDED.source,
                    status      = EXCLUDED.status,
                    scrape_date = EXCLUDED.scrape_date
                """
                
                success = self.execute_values(insert_query, match_data)
                
                if success:
                    logger.info(f"Imported {len(match_data)} matches from {csv_path}")
                    return len(match_data)
            
            return 0
            
        except Exception as e:
            logger.error(f"Error importing matches from CSV: {str(e)}")
            return 0
    
    def get_raw_match_data(self, match_id=None, team_id=None, date_from=None, date_to=None, limit=None):
        """
        Get raw match data without any processing.
        
        Args:
            match_id: Optional match ID to filter by
            team_id: Optional team ID to filter by
            date_from: Optional start date
            date_to: Optional end date
            limit: Optional limit on number of results
            
        Returns:
            List of raw match data rows
        """
        try:
            # Build query
            query = """
            SELECT 
                m.*
            FROM 
                match m
            WHERE 
                1=1
            """
            
            params = []
            
            # Apply filters
            if match_id:
                query += " AND m.match_id = %s"
                params.append(match_id)
            
            if team_id:
                query += " AND (m.team_id = %s OR m.opponent_id = %s)"
                params.extend([team_id, team_id])
            
            if date_from:
                query += " AND m.date >= %s"
                params.append(date_from)
            
            if date_to:
                query += " AND m.date <= %s"
                params.append(date_to)
            
            # Add ordering
            query += " ORDER BY m.date DESC"
            
            # Add limit
            if limit:
                query += " LIMIT %s"
                params.append(limit)
            
            # Execute query
            results = self.execute_query(query, tuple(params) if params else None)
            
            return results
            
        except Exception as e:
            logger.error(f"Error getting raw match data: {str(e)}")
            return []
    
    def get_raw_team_data(self, team_name=None, league_id=None):
        """
        Get raw team data without any processing.
        
        Args:
            team_name: Optional team name to filter by
            league_id: Optional league ID to filter by
            
        Returns:
            List of raw team data rows
        """
        try:
            # Build query
            query = """
            SELECT 
                t.*
            FROM 
                team t
            WHERE 
                1=1
            """
            
            params = []
            
            # Apply filters
            if team_name:
                query += " AND t.team_name = %s"
                params.append(team_name)
            
            if league_id:
                query += " AND t.league_id = %s"
                params.append(league_id)
            
            # Add ordering
            query += " ORDER BY t.team_name"
            
            # Execute query
            results = self.execute_query(query, tuple(params) if params else None)
            
            return results
            
        except Exception as e:
            logger.error(f"Error getting raw team data: {str(e)}")
            return []
    
    def get_raw_league_data(self, league_name=None):
        """
        Get raw league data without any processing.
        
        Args:
            league_name: Optional league name to filter by
            
        Returns:
            List of raw league data rows
        """
        try:
            # Build query
            query = """
            SELECT 
                l.*
            FROM 
                league l
            WHERE 
                1=1
            """
            
            params = []
            
            # Apply filters
            if league_name:
                query += " AND l.league_name = %s"
                params.append(league_name)
            
            # Add ordering
            query += " ORDER BY l.league_name"
            
            # Execute query
            results = self.execute_query(query, tuple(params) if params else None)
            
            return results
            
        except Exception as e:
            logger.error(f"Error getting raw league data: {str(e)}")
            return []
    
    def export_to_csv(self, query, output_path):
        """
        Export raw query results to a CSV file without any processing.
        
        Args:
            query: SQL query string
            output_path: Path to save the CSV file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Execute the query
            self.cur.execute(query)
            
            # Get column names from cursor description
            column_names = [desc[0] for desc in self.cur.description]
            
            # Fetch all rows
            rows = self.cur.fetchall()
            
            # Create DataFrame from query results
            df = pd.DataFrame(rows, columns=column_names)
            
            # Save to CSV
            df.to_csv(output_path, index=False)
            
            logger.info(f"Exported {len(df)} rows to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting to CSV: {str(e)}")
            return False
        
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