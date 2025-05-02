#!/usr/bin/env python3
"""
Football Database Initializer

This script initializes and manages the PostgreSQL database for football statistics:
1. Creates the database if it doesn't exist
2. Sets up the table schema
3. Imports data from existing CSV files
4. Provides utilities for backup and restore

Usage:
  python db_initializer.py --create-schema
  python db_initializer.py --import-data match_analysis/
  python db_initializer.py --reset-database
"""
import os
import sys
import argparse
import logging
import glob
import pandas as pd
from datetime import datetime
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import execute_values
import configparser
import subprocess
import time
import random

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('db_init.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatabaseInitializer:
    """Handles creation and initialization of the football statistics database"""
    
    def __init__(self, db_params):
        """Initialize with database connection parameters"""
        self.db_params = db_params
        self.conn = None
        self.cur = None
    
    def connect_postgres(self):
        """Connect to PostgreSQL (not to a specific database)"""
        try:
            # Connect to PostgreSQL server
            conn_params = {
                "host": self.db_params["host"],
                "port": self.db_params["port"]
            }
            
            # Add user and password if provided
            if self.db_params.get("user"):
                conn_params["user"] = self.db_params["user"]
            if self.db_params.get("password"):
                conn_params["password"] = self.db_params["password"]
            
            # Try to connect to 'postgres' database first (should exist on all installations)
            try:
                conn_params["dbname"] = "postgres"
                conn = psycopg2.connect(**conn_params)
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                logger.info("Connected to PostgreSQL server (postgres database)")
                return conn
            except Exception as e:
                logger.warning(f"Could not connect to postgres database: {str(e)}")
                
                # Try connecting without specifying a database
                del conn_params["dbname"]
                conn = psycopg2.connect(**conn_params)
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                logger.info("Connected to PostgreSQL server")
                return conn
            
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL: {str(e)}")
            return None
    
    def connect_database(self):
        """Connect to the specific database"""
        try:
            # Build connection string
            conn_params = {
                "dbname": self.db_params["dbname"],
                "host": self.db_params["host"],
                "port": self.db_params["port"]
            }
            
            # Add user and password if provided
            if self.db_params.get("user"):
                conn_params["user"] = self.db_params["user"]
            if self.db_params.get("password"):
                conn_params["password"] = self.db_params["password"]
            
            # Connect to the database
            self.conn = psycopg2.connect(**conn_params)
            self.cur = self.conn.cursor()
            
            logger.info(f"Connected to database: {self.db_params['dbname']}")
            return True
            
        except Exception as e:
            logger.error(f"Error connecting to database: {str(e)}")
            return False
    
    def disconnect(self):
        """Close the database connection"""
        if self.cur:
            self.cur.close()
            self.cur = None
        
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("Database connection closed")
    
    def database_exists(self):
        """Check if the database exists"""
        conn = self.connect_postgres()
        if not conn:
            return False
        
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (self.db_params["dbname"],))
            exists = cur.fetchone() is not None
            cur.close()
            conn.close()
            return exists
        except Exception as e:
            logger.error(f"Error checking if database exists: {str(e)}")
            if conn:
                conn.close()
            return False
    
    def create_database(self):
        """Create the database if it doesn't exist"""
        # Check if database already exists
        if self.database_exists():
            logger.info(f"Database {self.db_params['dbname']} already exists")
            return True
        
        # Connect to PostgreSQL
        conn = self.connect_postgres()
        if not conn:
            return False
        
        try:
            cur = conn.cursor()
            logger.info(f"Creating database '{self.db_params['dbname']}'...")
            cur.execute(f"CREATE DATABASE {self.db_params['dbname']}")
            cur.close()
            conn.close()
            
            logger.info(f"Created database: {self.db_params['dbname']}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating database: {str(e)}")
            if conn:
                conn.close()
            return False
    
    def drop_database(self):
        """Drop the database if it exists"""
        # Ensure no connections are active
        self.disconnect()
        
        # Check if database exists
        if not self.database_exists():
            logger.info(f"Database {self.db_params['dbname']} does not exist")
            return True
        
        # Connect to PostgreSQL
        conn = self.connect_postgres()
        if not conn:
            return False
        
        try:
            cur = conn.cursor()
            
            # Disable connections to the database
            cur.execute(f"""
                UPDATE pg_database SET datallowconn = FALSE WHERE datname = '{self.db_params["dbname"]}';
            """)
            
            # Terminate all existing connections
            cur.execute(f"""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = '{self.db_params["dbname"]}'
                AND pid <> pg_backend_pid();
            """)
            
            # Drop the database
            logger.info(f"Dropping database '{self.db_params['dbname']}'...")
            cur.execute(f"DROP DATABASE {self.db_params['dbname']}")
            
            cur.close()
            conn.close()
            
            logger.info(f"Dropped database: {self.db_params['dbname']}")
            return True
            
        except Exception as e:
            logger.error(f"Error dropping database: {str(e)}")
            if conn:
                conn.close()
            return False
    
    def execute_query(self, query, params=None, commit=True):
        """Execute a SQL query and return results"""
        if not self.conn or not self.cur:
            if not self.connect_database():
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
    
    def execute_batch(self, query, params_list, commit=True):
        """Execute a batch of queries with different parameters"""
        if not self.conn or not self.cur:
            if not self.connect_database():
                return False
        
        try:
            for params in params_list:
                self.cur.execute(query, params)
            
            if commit:
                self.conn.commit()
            
            return True
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error executing batch query: {str(e)}")
            logger.error(f"Query: {query}")
            return False
    
    def execute_values(self, query, values, commit=True):
        """Execute a query with multiple sets of values using psycopg2.extras.execute_values"""
        if not self.conn or not self.cur:
            if not self.connect_database():
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
        """Create the database schema"""
        logger.info("Creating database schema...")
        
        if not self.connect_database():
            return False
        
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
            
            # Create team_match_stats view
            self.execute_query("""
                CREATE OR REPLACE VIEW team_match_stats AS
                SELECT 
                    m.match_id,
                    m.date,
                    t1.team_name as team,
                    t2.team_name as opponent,
                    m.venue,
                    m.competition,
                    m.round,
                    m.result,
                    m.gf,
                    m.ga,
                    m.xg,
                    m.xga,
                    m.sh as shots,
                    m.sot as shots_on_target,
                    m.possession,
                    m.corners,
                    m.opp_corners,
                    l.league_name as league,
                    l.country,
                    m.scrape_date
                FROM match m
                JOIN team t1 ON m.team_id = t1.team_id
                JOIN team t2 ON m.opponent_id = t2.team_id
                LEFT JOIN league l ON t1.league_id = l.league_id
                ORDER BY m.date DESC, t1.team_name
            """)
            
            # Create team performance summary view
            self.execute_query("""
                CREATE OR REPLACE VIEW team_performance AS
                SELECT 
                    t.team_id,
                    t.team_name,
                    l.league_name,
                    COUNT(m.match_id) as matches_played,
                    SUM(CASE WHEN m.result = 'W' THEN 1 ELSE 0 END) as wins,
                    SUM(CASE WHEN m.result = 'D' THEN 1 ELSE 0 END) as draws,
                    SUM(CASE WHEN m.result = 'L' THEN 1 ELSE 0 END) as losses,
                    SUM(m.gf) as goals_for,
                    SUM(m.ga) as goals_against,
                    SUM(m.gf) - SUM(m.ga) as goal_difference,
                    AVG(m.xg) as avg_xg,
                    AVG(m.xga) as avg_xga,
                    AVG(m.possession) as avg_possession,
                    MAX(m.date) as last_match_date
                FROM team t
                LEFT JOIN match m ON t.team_id = m.team_id
                LEFT JOIN league l ON t.league_id = l.league_id
                GROUP BY t.team_id, t.team_name, l.league_name
                ORDER BY matches_played DESC, wins DESC
            """)
            
            logger.info("Database schema created successfully")
            self.disconnect()
            return True
            
        except Exception as e:
            logger.error(f"Error creating schema: {str(e)}")
            self.disconnect()
            return False
    
    def import_leagues_from_csv(self, df):
        """Import leagues from a DataFrame"""
        league_col = None
        for col in ['league', 'competition', 'comp']:
            if col in df.columns:
                league_col = col
                break
        
        if not league_col:
            logger.warning("No league column found in CSV")
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
            
            # Insert league
            query = """
                INSERT INTO league (league_name, country)
                VALUES (%s, %s)
                ON CONFLICT (league_name) DO NOTHING
            """
            
            result = self.execute_query(query, (league, country))
            if result:
                count += 1
        
        return count
    
    def import_teams_from_csv(self, df):
        """Import teams from a DataFrame"""
        # Check for team and opponent columns
        if 'team' not in df.columns or 'opponent' not in df.columns:
            logger.warning("Team or opponent column missing in CSV")
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
                    query = "SELECT league_id FROM league WHERE league_name = %s"
                    result = self.execute_query(query, (team_leagues[0],))
                    
                    if result and len(result) > 0:
                        league_id = result[0][0]
            
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
        
        return count
    
    def import_matches_from_csv(self, df):
        """Import matches from a DataFrame"""
        # Check for required columns
        if 'team' not in df.columns or 'opponent' not in df.columns or 'date' not in df.columns:
            logger.warning("Required columns missing in CSV")
            return 0
        
        # Ensure date is in datetime format
        df['date'] = pd.to_datetime(df['date'])
        
        # Create match_id if it doesn't exist
        if 'match_id' not in df.columns:
            df['match_id'] = df.apply(
                lambda row: f"{str(row['date'].date())}_{row['team']}_{row['opponent']}",
                axis=1
            )
        
        # Prepare matches for insertion
        match_data = []
        
        for _, row in df.iterrows():
            # Get team IDs
            team_name = row['team']
            opponent_name = row['opponent']
            
            # Query team IDs
            query = "SELECT team_id FROM team WHERE team_name = %s"
            team_result = self.execute_query(query, (team_name,))
            
            if not team_result or len(team_result) == 0:
                logger.warning(f"Team not found: {team_name}")
                continue
            
            opponent_result = self.execute_query(query, (opponent_name,))
            
            if not opponent_result or len(opponent_result) == 0:
                logger.warning(f"Team not found: {opponent_name}")
                continue
            
            team_id = team_result[0][0]
            opponent_id = opponent_result[0][0]
            
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
                row.get('sh', row.get('shots', None)),
                row.get('sot', row.get('shots_on_target', None)),
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
                row.get('source', 'csv_import')
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
            
            # Use execute_values for better performance
            success = self.execute_values(insert_query, match_data)
            
            if success:
                logger.info(f"Imported {len(match_data)} matches")
                return len(match_data)
        
        return 0
    
    def import_csv_file(self, csv_file):
        """Import data from a CSV file into the database"""
        if not os.path.exists(csv_file):
            logger.error(f"CSV file not found: {csv_file}")
            return False
        
        logger.info(f"Importing data from {csv_file}")
        
        try:
            # Read CSV file
            df = pd.read_csv(csv_file)
            
            if df.empty:
                logger.warning(f"CSV file is empty: {csv_file}")
                return False
            
            # Check for required columns
            required_cols = ['team', 'opponent']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                logger.error(f"CSV file missing required columns: {missing_cols}")
                return False
            
            # Make sure date column exists and is properly formatted
            if 'date' not in df.columns:
                logger.error("CSV file missing date column")
                return False
            
            # Connect to database
            if not self.connect_database():
                return False
            
            # Import leagues
            league_count = self.import_leagues_from_csv(df)
            logger.info(f"Imported {league_count} leagues")
            
            # Import teams
            team_count = self.import_teams_from_csv(df)
            logger.info(f"Imported {team_count} teams")
            
            # Import matches
            match_count = self.import_matches_from_csv(df)
            logger.info(f"Imported {match_count} matches")
            
            self.disconnect()
            return True
            
        except Exception as e:
            logger.error(f"Error importing CSV file: {str(e)}")
            self.disconnect()
            return False
    
    def import_data_from_directory(self, directory):
        """Import data from all CSV files in a directory"""
        if not os.path.exists(directory):
            logger.error(f"Directory not found: {directory}")
            return False
        
        # Get all CSV files in the directory (recursive)
        csv_files = []
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith('.csv'):
                    csv_files.append(os.path.join(root, file))
        
        if not csv_files:
            logger.warning(f"No CSV files found in {directory}")
            return False
        
        logger.info(f"Found {len(csv_files)} CSV files to import")
        
        # Make sure database is connected
        if not self.connect_database():
            return False
        
        # Process files in a specific order - import the most important files first
        important_patterns = ['team', 'league', 'fixtures', 'matches', 'combined']
        
        def get_file_priority(file_path):
            # Files with important patterns get higher priority (lower number)
            file_name = os.path.basename(file_path).lower()
            for i, pattern in enumerate(important_patterns):
                if pattern in file_name:
                    return i
            return len(important_patterns)  # Lowest priority
        
        # Sort files by priority
        csv_files.sort(key=get_file_priority)
        
        # Process each file
        success_count = 0
        total_matches = 0
        
        for csv_file in csv_files:
            logger.info(f"Processing {os.path.basename(csv_file)}")
            
            try:
                # Read the CSV file
                df = pd.read_csv(csv_file)
                
                if df.empty:
                    logger.warning(f"Empty CSV file: {os.path.basename(csv_file)}")
                    continue
                
                # Check if it has the required columns
                if not all(col in df.columns for col in ['team', 'opponent']):
                    logger.warning(f"Missing required columns in {os.path.basename(csv_file)}")
                    continue
                
                # Make sure date column exists and is properly formatted
                if 'date' not in df.columns:
                    logger.warning(f"Missing date column in {os.path.basename(csv_file)}")
                    continue
                
                # Import the data
                league_count = self.import_leagues_from_csv(df)
                team_count = self.import_teams_from_csv(df)
                match_count = self.import_matches_from_csv(df)
                
                if match_count > 0:
                    logger.info(f"Imported {match_count} matches from {os.path.basename(csv_file)}")
                    success_count += 1
                    total_matches += match_count
                
            except Exception as e:
                logger.error(f"Error processing {os.path.basename(csv_file)}: {str(e)}")
        
        # Disconnect from database
        self.disconnect()
        
        logger.info(f"Successfully imported data from {success_count} of {len(csv_files)} files")
        logger.info(f"Total matches imported: {total_matches}")
        
        return success_count > 0
    
    def backup_database(self, backup_dir="backups"):
        """
        Create a backup of the database using pg_dump
        
        Args:
            backup_dir: Directory to store the backup
            
        Returns:
            Path to the backup file or None if failed
        """
        # Create backup directory if it doesn't exist
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
        
        # Generate backup file name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = os.path.join(backup_dir, f"{self.db_params['dbname']}_{timestamp}.backup")
        
        try:
            # Build pg_dump command
            cmd = [
                "pg_dump",
                "-Fc",  # Custom format (compressed)
                "-f", backup_file
            ]
            
            # Add connection parameters
            if self.db_params.get("host"):
                cmd.extend(["-h", self.db_params["host"]])
            
            if self.db_params.get("port"):
                cmd.extend(["-p", str(self.db_params["port"])])
            
            if self.db_params.get("user"):
                cmd.extend(["-U", self.db_params["user"]])
            
            # Add database name
            cmd.append(self.db_params["dbname"])
            
            # Set PGPASSWORD environment variable if password is provided
            env = os.environ.copy()
            if self.db_params.get("password"):
                env["PGPASSWORD"] = self.db_params["password"]
            
            # Execute pg_dump
            logger.info(f"Creating database backup: {backup_file}")
            result = subprocess.run(cmd, env=env, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            if result.returncode == 0:
                logger.info(f"Backup created successfully: {backup_file}")
                return backup_file
            else:
                logger.error(f"pg_dump failed: {result.stderr.decode()}")
                return None
            
        except Exception as e:
            logger.error(f"Error creating backup: {str(e)}")
            return None
    
    def restore_database(self, backup_file):
        """
        Restore database from a backup file
        
        Args:
            backup_file: Path to the backup file
            
        Returns:
            True if successful, False otherwise
        """
        if not os.path.exists(backup_file):
            logger.error(f"Backup file not found: {backup_file}")
            return False
        
        try:
            # Make sure the database exists
            if not self.database_exists():
                logger.info(f"Database {self.db_params['dbname']} does not exist, creating it...")
                if not self.create_database():
                    return False
            
            # Build pg_restore command
            cmd = [
                "pg_restore",
                "-d", self.db_params["dbname"],
                "-c"  # Clean (drop objects before creating)
            ]
            
            # Add connection parameters
            if self.db_params.get("host"):
                cmd.extend(["-h", self.db_params["host"]])
            
            if self.db_params.get("port"):
                cmd.extend(["-p", str(self.db_params["port"])])
            
            if self.db_params.get("user"):
                cmd.extend(["-U", self.db_params["user"]])
            
            # Add backup file
            cmd.append(backup_file)
            
            # Set PGPASSWORD environment variable if password is provided
            env = os.environ.copy()
            if self.db_params.get("password"):
                env["PGPASSWORD"] = self.db_params["password"]
            
            # Execute pg_restore
            logger.info(f"Restoring database from backup: {backup_file}")
            result = subprocess.run(cmd, env=env, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            if result.returncode == 0:
                logger.info(f"Database restored successfully from {backup_file}")
                return True
            else:
                logger.error(f"pg_restore failed: {result.stderr.decode()}")
                return False
            
        except Exception as e:
            logger.error(f"Error restoring database: {str(e)}")
            return False
    
    def reset_database(self):
        """
        Reset the database (drop and recreate)
        
        Returns:
            True if successful, False otherwise
        """
        # Create backup first if database exists
        if self.database_exists():
            logger.info("Creating backup before resetting database...")
            self.backup_database()
        
        # Drop the database
        if not self.drop_database():
            return False
        
        # Create the database
        if not self.create_database():
            return False
        
        # Create the schema
        if not self.create_schema():
            return False
        
        logger.info("Database reset successfully")
        return True
    
    def get_database_stats(self):
        """
        Get database statistics
        
        Returns:
            Dictionary with database statistics
        """
        if not self.connect_database():
            return {}
        
        stats = {}
        
        try:
            # Get number of leagues
            result = self.execute_query("SELECT COUNT(*) FROM league")
            if result:
                stats['league_count'] = result[0][0]
            
            # Get number of teams
            result = self.execute_query("SELECT COUNT(*) FROM team")
            if result:
                stats['team_count'] = result[0][0]
            
            # Get number of matches
            result = self.execute_query("SELECT COUNT(*) FROM match")
            if result:
                stats['match_count'] = result[0][0]
            
            # Get date range
            result = self.execute_query("SELECT MIN(date), MAX(date) FROM match")
            if result:
                stats['min_date'] = str(result[0][0])
                stats['max_date'] = str(result[0][1])
            
            # Get top leagues
            result = self.execute_query("""
                SELECT l.league_name, COUNT(m.match_id) as match_count
                FROM league l
                JOIN team t ON l.league_id = t.league_id
                JOIN match m ON t.team_id = m.team_id
                GROUP BY l.league_name
                ORDER BY match_count DESC
                LIMIT 5
            """)
            if result:
                stats['top_leagues'] = [{'name': row[0], 'match_count': row[1]} for row in result]
            
            # Get top teams
            result = self.execute_query("""
                SELECT t.team_name, COUNT(m.match_id) as match_count
                FROM team t
                JOIN match m ON t.team_id = m.team_id
                GROUP BY t.team_name
                ORDER BY match_count DESC
                LIMIT 5
            """)
            if result:
                stats['top_teams'] = [{'name': row[0], 'match_count': row[1]} for row in result]
            
            self.disconnect()
            return stats
            
        except Exception as e:
            logger.error(f"Error getting database stats: {str(e)}")
            self.disconnect()
            return {}

def load_config(config_file="config.ini"):
    """
    Load configuration from file
    
    Args:
        config_file: Path to configuration file
        
    Returns:
        Dictionary with configuration
    """
    # Default configuration
    config = {
        "database": {
            "dbname": "fbref_stats",
            "user": "scraper_user",
            "password": "1759",
            "host": "localhost",
            "port": 5432
        }
    }
    
    # Try to load configuration from file
    if os.path.exists(config_file):
        try:
            parser = configparser.ConfigParser()
            parser.read(config_file)
            
            if "database" in parser:
                for key in parser["database"]:
                    if key == "port":
                        config["database"][key] = int(parser["database"][key])
                    else:
                        config["database"][key] = parser["database"][key]
            
            logger.info(f"Loaded configuration from {config_file}")
        except Exception as e:
            logger.error(f"Error loading configuration: {str(e)}")
    else:
        logger.warning(f"Configuration file {config_file} not found, using defaults")
    
    return config

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Football Database Initializer")
    
    # Database connection options
    parser.add_argument("--dbname", type=str, help="Database name")
    parser.add_argument("--user", type=str, help="Database user")
    parser.add_argument("--password", type=str, help="Database password")
    parser.add_argument("--host", type=str, help="Database host")
    parser.add_argument("--port", type=int, help="Database port")
    
    # Configuration option
    parser.add_argument("--config", type=str, default="config.ini", help="Path to configuration file")
    
    # Action options
    parser.add_argument("--create-database", action="store_true", help="Create the database")
    parser.add_argument("--create-schema", action="store_true", help="Create the database schema")
    parser.add_argument("--import-csv", type=str, help="Import data from a CSV file")
    parser.add_argument("--import-dir", type=str, help="Import data from a directory of CSV files")
    parser.add_argument("--backup", action="store_true", help="Create a database backup")
    parser.add_argument("--backup-dir", type=str, default="backups", help="Backup directory")
    parser.add_argument("--restore", type=str, help="Restore database from a backup file")
    parser.add_argument("--reset", action="store_true", help="Reset the database")
    parser.add_argument("--stats", action="store_true", help="Show database statistics")
    parser.add_argument("--full-init", type=str, help="Do full initialization with data from directory")
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Override configuration with command line arguments
    db_params = config["database"].copy()
    if args.dbname:
        db_params["dbname"] = args.dbname
    if args.user:
        db_params["user"] = args.user
    if args.password:
        db_params["password"] = args.password
    if args.host:
        db_params["host"] = args.host
    if args.port:
        db_params["port"] = args.port
    
    # Initialize database handler
    db_init = DatabaseInitializer(db_params)
    
    # Execute selected action
    if args.create_database:
        if db_init.create_database():
            logger.info(f"Database '{db_params['dbname']}' created successfully")
        else:
            logger.error(f"Failed to create database '{db_params['dbname']}'")
            return 1
    
    elif args.create_schema:
        if db_init.create_schema():
            logger.info("Database schema created successfully")
        else:
            logger.error("Failed to create database schema")
            return 1
    
    elif args.import_csv:
        if db_init.import_csv_file(args.import_csv):
            logger.info(f"Data imported successfully from {args.import_csv}")
        else:
            logger.error(f"Failed to import data from {args.import_csv}")
            return 1
    
    elif args.import_dir:
        if db_init.import_data_from_directory(args.import_dir):
            logger.info(f"Data imported successfully from {args.import_dir}")
        else:
            logger.error(f"Failed to import data from {args.import_dir}")
            return 1
    
    elif args.backup:
        backup_file = db_init.backup_database(args.backup_dir)
        if backup_file:
            logger.info(f"Database backup created successfully: {backup_file}")
        else:
            logger.error("Failed to create database backup")
            return 1
    
    elif args.restore:
        if db_init.restore_database(args.restore):
            logger.info(f"Database restored successfully from {args.restore}")
        else:
            logger.error(f"Failed to restore database from {args.restore}")
            return 1
    
    elif args.reset:
        if db_init.reset_database():
            logger.info("Database reset successfully")
        else:
            logger.error("Failed to reset database")
            return 1
    
    elif args.stats:
        stats = db_init.get_database_stats()
        if stats:
            print("\nDatabase Statistics:")
            print(f"Leagues: {stats.get('league_count', 0)}")
            print(f"Teams: {stats.get('team_count', 0)}")
            print(f"Matches: {stats.get('match_count', 0)}")
            print(f"Date range: {stats.get('min_date', 'N/A')} to {stats.get('max_date', 'N/A')}")
            
            if 'top_leagues' in stats:
                print("\nTop leagues:")
                for i, league in enumerate(stats['top_leagues'], 1):
                    print(f"  {i}. {league['name']} ({league['match_count']} matches)")
            
            if 'top_teams' in stats:
                print("\nTop teams:")
                for i, team in enumerate(stats['top_teams'], 1):
                    print(f"  {i}. {team['name']} ({team['match_count']} matches)")
        else:
            logger.error("Failed to get database statistics")
            return 1
    
    elif args.full_init:
        logger.info("Starting full database initialization...")
        
        # Create backup if database exists
        if db_init.database_exists():
            backup_file = db_init.backup_database(args.backup_dir)
            if backup_file:
                logger.info(f"Created backup before initialization: {backup_file}")
        
        # Reset database
        if not db_init.reset_database():
            logger.error("Failed to reset database")
            return 1
        
        # Import data
        if not db_init.import_data_from_directory(args.full_init):
            logger.error(f"Failed to import data from {args.full_init}")
            return 1
        
        # Show statistics
        stats = db_init.get_database_stats()
        if stats:
            print("\nDatabase initialized with:")
            print(f"  {stats.get('league_count', 0)} leagues")
            print(f"  {stats.get('team_count', 0)} teams")
            print(f"  {stats.get('match_count', 0)} matches")
        
        logger.info("Full database initialization completed successfully")
    
    else:
        # Show help if no action specified
        parser.print_help()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())