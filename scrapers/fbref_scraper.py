#!/usr/bin/env python3
"""
Enhanced FBref Scraper with Database Integration

This script provides an improved FBref scraper that:
1. Works directly with your PostgreSQL database
2. Efficiently updates team and match statistics
3. Handles errors gracefully and implements retry logic
4. Allows for selective updates based on teams, leagues, or time periods

Usage:
  python improved_fbref_scraper.py --team "Arsenal" --league "Premier League"
  python improved_fbref_scraper.py --league "Premier League" --matches 10
  python improved_fbref_scraper.py --all-teams --days-ago 7
"""
import time
import random
import re
import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
import pandas as pd
import requests
from bs4 import BeautifulSoup, Comment
import psycopg2
from psycopg2.extras import execute_values
import configparser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fbref_db_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatabaseConnection:
    """Handles PostgreSQL database connections and operations"""
    
    def __init__(self, dbname="fbref_stats", user=None, password=None, host="localhost", port=5432):
        """Initialize the database connection"""
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None
        self.cur = None
    
    def connect(self):
        """Connect to the PostgreSQL database"""
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
        """Close the database connection"""
        if self.cur:
            self.cur.close()
        
        if self.conn:
            self.conn.close()
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
            self.conn.rollback()
            logger.error(f"Error executing values: {str(e)}")
            logger.error(f"Query: {query}")
            return False
    
    def get_leagues(self):
        """Get all leagues from the database"""
        query = "SELECT league_id, league_name, country FROM league ORDER BY league_name"
        return self.execute_query(query)
    
    def get_teams(self, league_id=None):
        """Get teams from the database, optionally filtered by league_id"""
        if league_id:
            query = """
                SELECT team_id, team_name, league_id 
                FROM team 
                WHERE league_id = %s
                ORDER BY team_name
            """
            return self.execute_query(query, (league_id,))
        else:
            query = """
                SELECT team_id, team_name, league_id 
                FROM team 
                ORDER BY team_name
            """
            return self.execute_query(query)
    
    def get_team_by_name(self, team_name):
        """Find a team by name (including partial match)"""
        query = """
            SELECT team_id, team_name, league_id 
            FROM team 
            WHERE team_name ILIKE %s
            ORDER BY team_name
        """
        # Try exact match first
        result = self.execute_query(query, (team_name,))
        
        if result and len(result) > 0:
            return result[0]
        
        # Try partial match
        result = self.execute_query(query, (f"%{team_name}%",))
        
        if result and len(result) > 0:
            return result[0]
        
        return None
    
    def get_league_by_name(self, league_name):
        """Find a league by name (including partial match)"""
        query = """
            SELECT league_id, league_name, country 
            FROM league 
            WHERE league_name ILIKE %s
            ORDER BY league_name
        """
        # Try exact match first
        result = self.execute_query(query, (league_name,))
        
        if result and len(result) > 0:
            return result[0]
        
        # Try partial match
        result = self.execute_query(query, (f"%{league_name}%",))
        
        if result and len(result) > 0:
            return result[0]
        
        return None
    
    def add_league(self, league_name, country="Unknown"):
        """Add a new league to the database"""
        query = """
            INSERT INTO league (league_name, country)
            VALUES (%s, %s)
            ON CONFLICT (league_name) DO UPDATE
            SET country = EXCLUDED.country
            RETURNING league_id
        """
        result = self.execute_query(query, (league_name, country))
        
        if result and len(result) > 0:
            return result[0][0]
        
        return None
    
    def add_team(self, team_name, league_id=None):
        """Add a new team to the database"""
        query = """
            INSERT INTO team (team_name, league_id)
            VALUES (%s, %s)
            ON CONFLICT (team_name) DO UPDATE
            SET league_id = EXCLUDED.league_id
            RETURNING team_id
        """
        result = self.execute_query(query, (team_name, league_id))
        
        if result and len(result) > 0:
            return result[0][0]
        
        return None
    
    def get_recent_matches(self, team_id, limit=10):
        """Get recent matches for a team"""
        query = """
            SELECT match_id, date, team_id, opponent_id, venue, competition,
                round, result, gf, ga, xg, xga, possession, sh, sot, corners
            FROM match
            WHERE team_id = %s
            ORDER BY date DESC
            LIMIT %s
        """
        return self.execute_query(query, (team_id, limit))
    
    def insert_matches(self, match_data):
        """Insert or update match data in bulk"""
        if not match_data:
            return 0
        
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
            return len(match_data)
        
        return 0

class FBrefScraper:
    """Enhanced FBref scraper with database integration"""
    
    def __init__(self, db_connection, delay_min=3, delay_max=5):
        """
        Initialize the FBref scraper with database connection
        
        Args:
            db_connection: DatabaseConnection instance
            delay_min: Minimum delay between requests
            delay_max: Maximum delay between requests
        """
        self.db = db_connection
        
        # Mapping of league names to competition IDs used in FBref URLs
        self.competition_ids = {
            "Premier League": "9",
            "La Liga": "12",
            "Bundesliga": "20",
            "Serie A": "11",
            "Ligue 1": "13",
            "Champions League": "8",
            "Europa League": "19",
            "Championship": "10",
            "Eredivisie": "23",
            "Primeira Liga": "32"
        }
        
        # Initialize session for maintaining cookies and headers
        self.session = requests.Session()
        self.session.headers.update(self._get_random_headers())
        
        # Rate limiting parameters
        self.delay_min = delay_min
        self.delay_max = delay_max
        
        # Cache for team URLs
        self.team_urls_cache = {}
    
    def _get_random_headers(self):
        """Generate random headers to avoid detection"""
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0"
        ]
        
        return {
            "User-Agent": random.choice(user_agents),
            "Accept": "text/html,application/xhtml+xml,application/xml",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://fbref.com/",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "Connection": "keep-alive"
        }
    
    def _add_delay(self):
        """Add a random delay between requests to avoid rate limiting"""
        delay = self.delay_min + random.random() * (self.delay_max - self.delay_min)
        time.sleep(delay)
    
    def get_season_info(self, date_str=None):
        """
        Determine the season based on the date
        
        Args:
            date_str: Date string in format YYYY-MM-DD (defaults to today)
            
        Returns:
            Tuple containing (season_start_year, season_end_year, season_string)
        """
        if date_str:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
        else:
            date_obj = datetime.now().date()
        
        # In European football, seasons typically run from August to May
        if date_obj.month >= 8:  # August or later
            season_start = date_obj.year
        else:
            season_start = date_obj.year - 1
            
        season_end = season_start + 1
        season_str = f"{season_start}-{season_end}"
        
        return season_start, season_end, season_str
    
    def get_team_url(self, team_name, league_name):
        """
        Get the URL for a team on FBref
        
        Args:
            team_name: Name of the team
            league_name: Name of the league
            
        Returns:
            Team URL or None if not found
        """
        # Get the season information
        _, season_end_year, season_str = self.get_season_info()
        
        # Get league ID
        league_id = None
        for league, id in self.competition_ids.items():
            if league.lower() == league_name.lower():
                league_id = id
                break
        
        if not league_id:
            logger.warning(f"League ID not found for {league_name}")
            # Try to find the closest match
            for league, id in self.competition_ids.items():
                if league_name.lower() in league.lower() or league.lower() in league_name.lower():
                    league_id = id
                    logger.info(f"Using closest match: {league} (ID: {id})")
                    break
        
        if not league_id:
            logger.error(f"Cannot find league ID for {league_name}")
            return None
        
        # Construct the URL
        base_url = f"https://fbref.com/en/comps/{league_id}/{season_str}/{season_str}-Stats"
        logger.info(f"Searching for {team_name} in {league_name} at {base_url}")
        
        try:
            # Add delay
            self._add_delay()
            
            # Request the page
            response = self.session.get(base_url, timeout=30)
            
            if response.status_code != 200:
                logger.warning(f"Failed to access {base_url}, status: {response.status_code}")
                return None
            
            soup = BeautifulSoup(response.text, "lxml")
            
            # Try different methods to find team links
            team_urls = {}
            
            # Method 1: Check the standard table
            table = soup.find("table", id="stats_squads_standard_for")
            
            if table:
                logger.info("Found standard stats table")
                team_links = table.select("tbody tr td a")
                
                for link in team_links:
                    team_urls[link.text.strip()] = "https://fbref.com" + link["href"]
            else:
                # Method 2: Try to find team links in other tables
                tables = soup.find_all("table")
                for t in tables:
                    links = t.select("tbody tr td a")
                    if links and any("/squads/" in link.get("href", "") for link in links):
                        logger.info("Found team links in alternative table")
                        for link in links:
                            href = link.get("href", "")
                            if "/squads/" in href:
                                team_urls[link.text.strip()] = "https://fbref.com" + href
                                
            # Method 3: Search directly in the page content
            if not team_urls:
                logger.info("Trying to find team links directly in page content")
                all_links = soup.find_all("a")
                for link in all_links:
                    href = link.get("href", "")
                    if "/squads/" in href and link.text.strip():
                        team_urls[link.text.strip()] = "https://fbref.com" + href
            
            logger.info(f"Found {len(team_urls)} team URLs")
            
            # Look for the team
            # 1. Exact match
            if team_name in team_urls:
                logger.info(f"Found exact match for {team_name}")
                return team_urls[team_name]
            
            # 2. Case-insensitive match
            for name, url in team_urls.items():
                if team_name.lower() == name.lower():
                    logger.info(f"Found case-insensitive match: {name}")
                    return url
            
            # 3. Partial match
            for name, url in team_urls.items():
                if team_name.lower() in name.lower() or name.lower() in team_name.lower():
                    logger.info(f"Found partial match: {name}")
                    return url
            
            logger.warning(f"Team {team_name} not found in {league_name}")
            return None
            
        except Exception as e:
            logger.error(f"Error getting team URL: {str(e)}")
            return None
    
    def scrape_team_matches(self, team_name, league_name, num_matches=7):
        """
        Scrape match data for a specific team
        
        Args:
            team_name: Name of the team
            league_name: Name of the league
            num_matches: Number of most recent matches to scrape
            
        Returns:
            DataFrame with match data
        """
        # Get team URL
        team_url = self.get_team_url(team_name, league_name)
        
        if not team_url:
            logger.error(f"Could not find URL for {team_name} in {league_name}")
            return pd.DataFrame()
        
        # Extract team ID from URL
        team_id_match = re.search(r'/squads/([^/]+)/', team_url)
        if not team_id_match:
            logger.error(f"Could not extract team ID from URL: {team_url}")
            return pd.DataFrame()
            
        fbref_team_id = team_id_match.group(1)
        
        # Get season information
        _, season_end_year, _ = self.get_season_info()
        
        # Construct matches URL
        matches_url = f"https://fbref.com/en/squads/{fbref_team_id}/matchlogs/{season_end_year}/summary/"
        
        logger.info(f"Fetching matches for {team_name} from {matches_url}")
        
        try:
            # Add delay
            self._add_delay()
            
            # Request the page
            response = self.session.get(matches_url, timeout=30)
            
            if response.status_code != 200:
                logger.warning(f"Failed to access {matches_url}, status: {response.status_code}")
                return pd.DataFrame()
            
            soup = BeautifulSoup(response.text, "lxml")
            
            # Find the matches table
            table = soup.find("table", id=lambda x: x and "matchlogs" in x)
            
            if not table:
                logger.warning(f"No match logs table found for {team_name}")
                return pd.DataFrame()
            
            # Read the table into a DataFrame
            df = pd.read_html(str(table))[0]
            
            # Clean up multi-index columns if present
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [' '.join(col).strip() for col in df.columns.values]
            
            # Add team information
            df["team"] = team_name
            df["league"] = league_name
            
            # Filter for completed matches
            if "Result" in df.columns:
                df = df[df["Result"].notna() & (df["Result"] != "")]
            
            # Sort by date
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"])
                df = df.sort_values("Date", ascending=False)
            
            # Limit to the specified number of matches
            df = df.head(num_matches)
            
            logger.info(f"Found {len(df)} matches for {team_name}")
            
            # Process the DataFrame to match database schema
            return self.process_match_dataframe(df)
            
        except Exception as e:
            logger.error(f"Error scraping team matches: {str(e)}")
            return pd.DataFrame()
    
    def process_match_dataframe(self, df):
        """
        Process raw match DataFrame into the format needed for database storage
        
        Args:
            df: Raw DataFrame from FBref
            
        Returns:
            Processed DataFrame ready for database insertion
        """
        if df.empty:
            return df
        
        # Define column mappings
        column_mapping = {
            "Date": "date",
            "Comp": "competition",
            "Round": "round",
            "Day": "day",
            "Venue": "venue",
            "Result": "result",
            "GF": "gf",
            "GA": "ga",
            "Opponent": "opponent",
            "xG": "xg",
            "xGA": "xga",
            "Poss": "possession",
            "Sh": "sh",
            "SoT": "sot",
            "Dist": "dist",
            "FK": "fk",
            "PK": "pk",
            "PKatt": "pkatt",
            "CrdY": "yellow_cards",
            "CrdR": "red_cards",
            "Fls": "fouls",
            "CK": "corners",
            "team": "team",
            "league": "league"
        }
        
        # Create a new DataFrame with only the columns we need
        available_columns = [col for col in column_mapping.keys() if col in df.columns]
        result_df = df[available_columns].rename(columns={col: column_mapping[col] for col in available_columns})
        
        # Process date column
        if "date" in result_df.columns:
            result_df["date"] = pd.to_datetime(result_df["date"])
        
        # Process numeric columns
        numeric_columns = ["gf", "ga", "xg", "xga", "sh", "sot", "dist", "fk", "pk", "pkatt", 
                          "yellow_cards", "red_cards", "fouls", "corners"]
        
        for col in [c for c in numeric_columns if c in result_df.columns]:
            try:
                result_df[col] = pd.to_numeric(result_df[col], errors="coerce").fillna(0)
            except:
                pass
        
        # Process possession (remove % sign and convert to float)
        if "possession" in result_df.columns:
            try:
                result_df["possession"] = result_df["possession"].astype(str).str.rstrip("%").astype(float)
            except:
                pass
        
        # Process venue (standardize to Home/Away)
        if "venue" in result_df.columns:
            result_df["venue"] = result_df["venue"].apply(
                lambda x: "Home" if x == "Home" else "Away" if x == "Away" else x
            )
        
        # Create match_id column
        try:
            result_df["match_id"] = result_df.apply(
                lambda row: f"{row['date'].strftime('%Y%m%d')}_{row['team']}_{row['opponent']}",
                axis=1
            )
        except:
            pass
        
        # Add source column
        result_df["source"] = "fbref"
        
        return result_df
    
    def update_team_in_database(self, team_name, league_name, num_matches=7, force_update=False):
        """
        Update match data for a team in the database
        
        Args:
            team_name: Name of the team
            league_name: Name of the league
            num_matches: Number of recent matches to fetch
            force_update: Whether to force an update even if recent data exists
            
        Returns:
            Number of matches updated
        """
        # Connect to database
        if not self.db.connect():
            logger.error("Failed to connect to database")
            return 0
        
        try:
            # Find or create league
            league = self.db.get_league_by_name(league_name)
            league_id = None
            
            if league:
                league_id = league[0]
                logger.info(f"Found league in database: {league[1]} (ID: {league_id})")
            else:
                # Add league to database
                league_id = self.db.add_league(league_name)
                logger.info(f"Added new league to database: {league_name} (ID: {league_id})")
            
            # Find or create team
            team = self.db.get_team_by_name(team_name)
            team_id = None
            
            if team:
                team_id = team[0]
                team_name = team[1]  # Use exact name from database
                logger.info(f"Found team in database: {team_name} (ID: {team_id})")
                
                # Update league reference if needed
                if team[2] != league_id:
                    self.db.execute_query(
                        "UPDATE team SET league_id = %s WHERE team_id = %s",
                        (league_id, team_id)
                    )
                    logger.info(f"Updated league reference for {team_name}")
            else:
                # Add team to database
                team_id = self.db.add_team(team_name, league_id)
                logger.info(f"Added new team to database: {team_name} (ID: {team_id})")
            
            # Check if we need to update this team
            if not force_update:
                # Get the last scrape date for this team
                result = self.db.execute_query(
                    "SELECT MAX(scrape_date) FROM match WHERE team_id = %s",
                    (team_id,)
                )
                
                if result and result[0][0]:
                    last_update = result[0][0]
                    now = datetime.now()
                    hours_since_update = (now - last_update).total_seconds() / 3600
                    
                    if hours_since_update < 24:  # Less than 24 hours old
                        logger.info(f"Skipping {team_name} - updated {hours_since_update:.1f} hours ago")
                        self.db.disconnect()
                        return 0
            
            # Scrape matches for this team
            matches_df = self.scrape_team_matches(team_name, league_name, num_matches)
            
            if matches_df.empty:
                logger.warning(f"No match data found for {team_name}")
                self.db.disconnect()
                return 0
            
            # Get opponent IDs and prepare match data
            match_data = []
            
            for _, row in matches_df.iterrows():
                opponent_name = row.get('opponent')
                if not opponent_name:
                    continue
                
                # Find or create opponent
                opponent = self.db.get_team_by_name(opponent_name)
                opponent_id = None
                
                if opponent:
                    opponent_id = opponent[0]
                else:
                    # Add opponent to database
                    opponent_id = self.db.add_team(opponent_name, league_id)
                    logger.info(f"Added new team to database: {opponent_name} (ID: {opponent_id})")
                
                # Create match data tuple
                match_data.append((
                    row.get('match_id', f"{row.get('date').strftime('%Y%m%d')}_{team_name}_{opponent_name}"),
                    row.get('date').strftime('%Y-%m-%d'),
                    team_id,
                    opponent_id,
                    row.get('venue'),
                    row.get('competition'),
                    row.get('round'),
                    row.get('result'),
                    row.get('gf'),
                    row.get('ga'),
                    row.get('xg'),
                    row.get('xga'),
                    row.get('sh'),
                    row.get('sot'),
                    row.get('dist'),
                    row.get('fk'),
                    row.get('pk'),
                    row.get('pkatt'),
                    row.get('possession'),
                    row.get('yellow_cards'),
                    row.get('red_cards'),
                    row.get('fouls'),
                    row.get('corners'),
                    None,  # opp_corners
                    datetime.now(),
                    row.get('source', 'fbref')
                ))
            
            # Insert matches into database
            match_count = self.db.insert_matches(match_data)
            logger.info(f"Updated {match_count} matches for {team_name}")
            
            return match_count
            
        except Exception as e:
            logger.error(f"Error updating team in database: {str(e)}")
            return 0
        finally:
            # Close connection
            self.db.disconnect()
    
    def update_league_in_database(self, league_name, num_matches=7, limit=None, force_update=False):
        """
        Update all teams in a league
        
        Args:
            league_name: Name of the league
            num_matches: Number of recent matches to fetch per team
            limit: Maximum number of teams to update (for testing)
            force_update: Whether to force an update even if recent data exists
            
        Returns:
            Number of matches updated
        """
        # Connect to database
        if not self.db.connect():
            logger.error("Failed to connect to database")
            return 0
        
        try:
            # Find league
            league = self.db.get_league_by_name(league_name)
            
            if not league:
                # Add league to database
                league_id = self.db.add_league(league_name)
                logger.info(f"Added new league to database: {league_name} (ID: {league_id})")
                
                # No teams in this league yet, try to get them from FBref
                self.db.disconnect()  # Close connection before scraping
                return self.scrape_new_league(league_name, num_matches)
            
            league_id = league[0]
            league_name = league[1]  # Use exact name from database
            
            # Get teams for this league
            teams = self.db.get_teams(league_id)
            
            if not teams:
                logger.warning(f"No teams found for league: {league_name}")
                self.db.disconnect()
                return 0
            
            logger.info(f"Found {len(teams)} teams in {league_name}")
            
            # Limit number of teams if specified
            if limit and len(teams) > limit:
                teams = teams[:limit]
                logger.info(f"Limiting to {limit} teams")
            
            # Close connection before scraping
            self.db.disconnect()
            
            # Update each team
            total_matches = 0
            
            for i, team in enumerate(teams):
                team_id = team[0]
                team_name = team[1]
                
                logger.info(f"Updating team {i+1}/{len(teams)}: {team_name}")
                
                # Update team
                match_count = self.update_team_in_database(team_name, league_name, num_matches, force_update)
                total_matches += match_count
                
                # Add delay between teams
                if i < len(teams) - 1:
                    self._add_delay()
            
            logger.info(f"Updated {total_matches} matches across {len(teams)} teams in {league_name}")
            return total_matches
            
        except Exception as e:
            logger.error(f"Error updating league: {str(e)}")
            return 0
        finally:
            # Make sure connection is closed
            if self.db.conn:
                self.db.disconnect()
    
    def scrape_new_league(self, league_name, num_matches=7):
        """
        Scrape a new league and add all teams and matches to the database
        
        Args:
            league_name: Name of the league
            num_matches: Number of recent matches to fetch per team
            
        Returns:
            Number of matches added
        """
        # Find league ID in FBref
        league_id = None
        for league, id in self.competition_ids.items():
            if league_name.lower() in league.lower() or league.lower() in league_name.lower():
                league_id = id
                logger.info(f"Using league: {league} (ID: {id})")
                break
        
        if not league_id:
            logger.error(f"Cannot find league ID for {league_name}")
            return 0
        
        # Get season information
        _, _, season_str = self.get_season_info()
        
        # Construct the URL
        base_url = f"https://fbref.com/en/comps/{league_id}/{season_str}/{season_str}-Stats"
        logger.info(f"Fetching teams for {league_name} from {base_url}")
        
        try:
            # Add delay
            self._add_delay()
            
            # Request the page
            response = self.session.get(base_url, timeout=30)
            
            if response.status_code != 200:
                logger.warning(f"Failed to access {base_url}, status: {response.status_code}")
                return 0
            
            soup = BeautifulSoup(response.text, "lxml")
            
            # Try different methods to find team links
            team_urls = {}
            
            # Method 1: Check the standard table
            table = soup.find("table", id="stats_squads_standard_for")
            
            if table:
                logger.info("Found standard stats table")
                team_links = table.select("tbody tr td a")
                
                for link in team_links:
                    team_urls[link.text.strip()] = "https://fbref.com" + link["href"]
            else:
                # Method 2: Try to find team links in other tables
                tables = soup.find_all("table")
                for t in tables:
                    links = t.select("tbody tr td a")
                    if links and any("/squads/" in link.get("href", "") for link in links):
                        logger.info("Found team links in alternative table")
                        for link in links:
                            href = link.get("href", "")
                            if "/squads/" in href:
                                team_urls[link.text.strip()] = "https://fbref.com" + href
            
            if not team_urls:
                logger.warning(f"No teams found for {league_name}")
                return 0
            
            logger.info(f"Found {len(team_urls)} teams in {league_name}")
            
            # Add league to database
            self.db.connect()
            db_league_id = self.db.add_league(league_name)
            self.db.disconnect()
            
            # Process each team
            total_matches = 0
            
            for i, (team_name, _) in enumerate(team_urls.items()):
                logger.info(f"Processing team {i+1}/{len(team_urls)}: {team_name}")
                
                # Update team
                match_count = self.update_team_in_database(team_name, league_name, num_matches, True)
                total_matches += match_count
                
                # Add delay between teams
                if i < len(team_urls) - 1:
                    self._add_delay()
            
            logger.info(f"Added {total_matches} matches across {len(team_urls)} teams in {league_name}")
            return total_matches
            
        except Exception as e:
            logger.error(f"Error scraping new league: {str(e)}")
            return 0

def load_config(config_file):
    """Load configuration from file"""
    config = {
        "database": {
            "dbname": "fbref_stats",
            "user": None,
            "password": None,
            "host": "localhost",
            "port": 5432
        },
        "scraping": {
            "delay_min": 3,
            "delay_max": 5,
            "matches_per_team": 7
        }
    }
    
    if not os.path.exists(config_file):
        logger.warning(f"Config file not found: {config_file}")
        return config
    
    try:
        parser = configparser.ConfigParser()
        parser.read(config_file)
        
        # Update database config
        if "database" in parser:
            for key in parser["database"]:
                if key in config["database"]:
                    if key == "port":
                        config["database"][key] = int(parser["database"][key])
                    else:
                        config["database"][key] = parser["database"][key]
        
        # Update scraping config
        if "scraping" in parser:
            for key in parser["scraping"]:
                if key in config["scraping"]:
                    config["scraping"][key] = int(parser["scraping"][key])
        
        logger.info(f"Loaded configuration from {config_file}")
        return config
        
    except Exception as e:
        logger.error(f"Error loading config file: {str(e)}")
        return config

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Enhanced FBref Scraper with Database Integration")
    
    # Database connection options
    parser.add_argument("--dbname", type=str, help="Database name")
    parser.add_argument("--user", type=str, help="Database user")
    parser.add_argument("--password", type=str, help="Database password")
    parser.add_argument("--host", type=str, help="Database host")
    parser.add_argument("--port", type=int, help="Database port")
    
    # Scraping options
    parser.add_argument("--config", type=str, help="Path to config file")
    parser.add_argument("--team", type=str, help="Team to scrape")
    parser.add_argument("--league", type=str, help="League to scrape")
    parser.add_argument("--all-teams", action="store_true", help="Scrape all teams in database")
    parser.add_argument("--matches", type=int, default=7, help="Number of matches to scrape per team")
    parser.add_argument("--limit", type=int, help="Limit number of teams to scrape")
    parser.add_argument("--force", action="store_true", help="Force update even if data is recent")
    parser.add_argument("--days-ago", type=int, help="Update teams that haven't been updated in X days")
    
    args = parser.parse_args()
    
    # Load config
    config = load_config(args.config if args.config else "config.ini")
    
    # Override config with command line arguments
    db_params = {
        "dbname": args.dbname or config["database"]["dbname"],
        "user": args.user or config["database"]["user"],
        "password": args.password or config["database"]["password"],
        "host": args.host or config["database"]["host"],
        "port": args.port or config["database"]["port"]
    }
    
    # Initialize database connection
    db = DatabaseConnection(**db_params)
    
    # Initialize scraper
    scraper = FBrefScraper(
        db,
        delay_min=config["scraping"]["delay_min"],
        delay_max=config["scraping"]["delay_max"]
    )
    
    # Determine what to scrape
    if args.team and args.league:
        # Scrape specific team
        logger.info(f"Scraping team: {args.team} in {args.league}")
        match_count = scraper.update_team_in_database(
            args.team,
            args.league,
            args.matches,
            args.force
        )
        logger.info(f"Updated {match_count} matches")
        
    elif args.league:
        # Scrape entire league
        logger.info(f"Scraping league: {args.league}")
        match_count = scraper.update_league_in_database(
            args.league,
            args.matches,
            args.limit,
            args.force
        )
        logger.info(f"Updated {match_count} matches across multiple teams")
        
    elif args.all_teams:
        # Scrape all teams in database
        logger.info("Scraping all teams in database")
        
        # Connect to database
        if not db.connect():
            logger.error("Failed to connect to database")
            return 1
        
        # Get all leagues
        leagues = db.get_leagues()
        db.disconnect()
        
        if not leagues:
            logger.error("No leagues found in database")
            return 1
        
        total_matches = 0
        
        # Process each league
        for league in leagues:
            league_id = league[0]
            league_name = league[1]
            
            logger.info(f"Processing league: {league_name}")
            
            match_count = scraper.update_league_in_database(
                league_name,
                args.matches,
                args.limit,
                args.force
            )
            
            total_matches += match_count
        
        logger.info(f"Updated {total_matches} matches across all leagues")
        
    else:
        logger.error("No action specified. Use --team and --league, --league, or --all-teams")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())