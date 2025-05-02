import time
import re
import os
import logging
import random
import requests
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Union, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fbref_scraper.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class FBrefScraper:
    """
    Enhanced scraper for FBref football match data focusing on historical match statistics
    """
    
    def __init__(self, delay_min=3, delay_max=5):
        """
        Initialize the FBref scraper
        
        Args:
            delay_min: Minimum delay between requests in seconds
            delay_max: Maximum delay between requests in seconds
        """
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
        
        # Storage for team URLs by league
        self.team_urls_cache = {}
    
    def _get_random_headers(self) -> Dict[str, str]:
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
    
    def _add_delay(self) -> None:
        """Add a random delay between requests to avoid rate limiting"""
        delay = self.delay_min + random.random() * (self.delay_max - self.delay_min)
        time.sleep(delay)
    
    def get_season_info(self, date_str: str) -> Tuple[int, int, str]:
        """
        Determine the season based on the date
        
        Args:
            date_str: Date string in format YYYY-MM-DD
            
        Returns:
            Tuple containing (season_start_year, season_end_year, season_string)
        """
        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
        
        # In European football, seasons typically run from August to May
        if date_obj.month >= 8:  # August or later
            season_start = date_obj.year
        else:
            season_start = date_obj.year - 1
            
        season_end = season_start + 1
        season_str = f"{season_start}-{season_end}"
        
        return season_start, season_end, season_str
    
    def get_team_urls(self, league_name: str, season_str: str) -> List[Dict[str, str]]:
        """
        Get all team URLs for a specific league and season
        
        Args:
            league_name: Name of the league
            season_str: Season string in format "YYYY-YYYY"
            
        Returns:
            List of dictionaries with team information including URLs
        """
        # Check cache first
        cache_key = f"{league_name}_{season_str}"
        if cache_key in self.team_urls_cache:
            return self.team_urls_cache[cache_key]
        
        if league_name not in self.competition_ids:
            logger.error(f"Unknown league: {league_name}")
            return []
            
        league_id = self.competition_ids[league_name]
        league_url = f"https://fbref.com/en/comps/{league_id}/{season_str}/{season_str}-Stats"
        
        logger.info(f"Fetching teams from {league_url}")
        
        try:
            # Add delay to avoid rate limiting
            self._add_delay()
            
            # Request the page
            response = self.session.get(league_url, timeout=30)
            
            if response.status_code != 200:
                logger.warning(f"Failed to fetch teams: HTTP {response.status_code}")
                return []
            
            soup = BeautifulSoup(response.text, "lxml")
            table = soup.find("table", id="stats_squads_standard_for")
            
            if not table:
                logger.warning(f"No teams table found at {league_url}")
                return []
            
            # Read team names from table
            teams_df = pd.read_html(str(table))[0]
            
            # Extract team links
            team_links = table.select("tbody tr td a")
            team_urls = ["https://fbref.com" + a["href"] for a in team_links]
            
            # Create team info dictionaries
            team_info = []
            for i, url in enumerate(team_urls):
                team_name = teams_df.iloc[i, 0] if i < len(teams_df) else "Unknown"
                team_id = url.split("/")[5]  # Extract hex ID from URL
                
                team_info.append({
                    "team_name": team_name,
                    "team_url": url,
                    "team_id": team_id,
                    "league": league_name,
                    "season": season_str
                })
            
            # Cache the results
            self.team_urls_cache[cache_key] = team_info
            
            logger.info(f"Found {len(team_info)} teams for {league_name} ({season_str})")
            return team_info
            
        except Exception as e:
            logger.error(f"Error fetching team URLs: {str(e)}")
            return []
    
    def scrape_team_match_logs(self, team_info: Dict[str, str]) -> pd.DataFrame:
        """
        Scrape all match logs for a specific team
        
        Args:
            team_info: Dictionary with team information including URL and ID
            
        Returns:
            DataFrame with match logs or empty DataFrame if failed
        """
        team_id = team_info["team_id"]
        team_name = team_info["team_name"]
        season_end_year = team_info["season"].split("-")[1]
        
        logs_url = f"https://fbref.com/en/squads/{team_id}/matchlogs/{season_end_year}/summary/"
        
        logger.info(f"Fetching match logs for {team_name} from {logs_url}")
        
        try:
            # Add delay to avoid rate limiting
            self._add_delay()
            
            # Request the page
            response = self.session.get(logs_url, timeout=30)
            
            if response.status_code != 200:
                logger.warning(f"Failed to fetch match logs: HTTP {response.status_code}")
                return pd.DataFrame()
            
            soup = BeautifulSoup(response.text, "lxml")
            
            # Find the table whose id contains "matchlogs"
            table = soup.find("table", id=lambda x: x and "matchlogs" in x)
            
            if not table:
                logger.warning(f"No match logs table found for {team_name}")
                return pd.DataFrame()
            
            # Read the table into a DataFrame
            df = pd.read_html(str(table))[0]
            
            # Add team information
            df["team"] = team_name
            df["season"] = team_info["season"]
            df["league"] = team_info["league"]
            
            # Clean up multi-index columns if present
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [' '.join(col).strip() for col in df.columns.values]
            
            logger.info(f"Found {len(df)} matches for {team_name}")
            return df
            
        except Exception as e:
            logger.error(f"Error scraping match logs for {team_name}: {str(e)}")
            return pd.DataFrame()
    
    def get_matches_by_date(self, target_date: str, leagues: List[str] = None) -> pd.DataFrame:
        """
        Get all matches for a specific date
        
        Args:
            target_date: Date string in format YYYY-MM-DD
            leagues: Optional list of league names to filter by
            
        Returns:
            DataFrame with matches for the target date
        """
        logger.info(f"Fetching matches for date: {target_date}")
        
        # Get season information
        _, _, season_str = self.get_season_info(target_date)
        target_date_obj = datetime.strptime(target_date, "%Y-%m-%d").date()
        
        # If no leagues specified, use all available
        if not leagues:
            leagues = list(self.competition_ids.keys())
        
        # Filter to only include leagues we have IDs for
        leagues = [league for league in leagues if league in self.competition_ids]
        
        all_matches = []
        
        # Process each league
        for league in leagues:
            # Get team URLs for this league and season
            team_info_list = self.get_team_urls(league, season_str)
            
            for team_info in team_info_list:
                # Get match logs for this team
                matches_df = self.scrape_team_match_logs(team_info)
                
                if matches_df.empty:
                    continue
                
                # Convert date column to datetime
                if "Date" in matches_df.columns:
                    matches_df["Date"] = pd.to_datetime(matches_df["Date"])
                
                # Filter for the target date
                date_matches = matches_df[matches_df["Date"].dt.date == target_date_obj]
                
                if not date_matches.empty:
                    all_matches.append(date_matches)
        
        # Combine all matches
        if all_matches:
            combined_df = pd.concat(all_matches, ignore_index=True)
            
            # Remove duplicate matches (same teams, same date)
            combined_df = combined_df.drop_duplicates(subset=["Date", "team", "Opponent"])
            
            logger.info(f"Found {len(combined_df)} matches for {target_date}")
            return combined_df
        else:
            logger.warning(f"No matches found for {target_date}")
            return pd.DataFrame()
    
    def process_match_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process and clean up the match data DataFrame
        
        Args:
            df: Raw DataFrame from FBref
            
        Returns:
            Processed DataFrame with standardized columns
        """
        if df.empty:
            return df
        
        # Define column mappings
        column_mapping = {
            "Date": "date",
            "Comp": "competition",
            "Round": "round",
            "Venue": "venue",
            "Opponent": "opponent",
            "Result": "result",
            "GF": "gf",
            "GA": "ga",
            "xG": "xg",
            "xGA": "xga",
            "Sh": "sh",
            "SoT": "sot",
            "FK": "fk",
            "PK": "pk",
            "PKatt": "pkatt",
            "Poss": "possession",
            "CrdY": "yellow_cards",
            "CrdR": "red_cards",
            "Fls": "fouls",
            "Crs": "corners",
            "team": "team",
            "season": "season",
            "league": "league"
        }
        
        # Select available columns and rename
        available_columns = [col for col in column_mapping.keys() if col in df.columns]
        renamed_mapping = {col: column_mapping[col] for col in available_columns}
        
        # Create new DataFrame with selected columns
        result_df = df[available_columns].rename(columns=renamed_mapping)
        
        # Process date column
        if "date" in result_df.columns:
            result_df["date"] = pd.to_datetime(result_df["date"])
        
        # Process numeric columns
        numeric_columns = ["gf", "ga", "sh", "sot", "fk", "pk", "pkatt", "corners", 
                           "yellow_cards", "red_cards", "fouls"]
        
        for col in [c for c in numeric_columns if c in result_df.columns]:
            try:
                result_df[col] = pd.to_numeric(result_df[col], errors="coerce").fillna(0).astype(int)
            except:
                pass
        
        # Process possession
        if "possession" in result_df.columns:
            try:
                result_df["possession"] = result_df["possession"].astype(str).str.rstrip("%").astype(float)
            except:
                pass
        
        # Process venue column to standardize format (Home/Away)
        if "venue" in result_df.columns:
            result_df["venue"] = result_df["venue"].apply(
                lambda x: "Home" if x == "Home" else "Away" if x == "Away" else x
            )
        
        # Add unique match_id based on date, teams and competition
        try:
            result_df["match_id"] = result_df.apply(
                lambda row: f"{row['date'].strftime('%Y%m%d')}_{row['team']}_{row['opponent']}_{row['competition']}",
                axis=1
            )
        except:
            pass
        
        # Add source column
        result_df["source"] = "fbref"
        
        return result_df
    
    def fetch_matches_for_date(self, target_date: str, leagues: List[str] = None) -> List[Dict]:
        """
        Fetch all matches for a specific date
        
        Args:
            target_date: Date string in format YYYY-MM-DD
            leagues: Optional list of league names to filter by
            
        Returns:
            List of match dictionaries
        """
        # Get matches DataFrame
        matches_df = self.get_matches_by_date(target_date, leagues)
        
        # Process the DataFrame
        processed_df = self.process_match_dataframe(matches_df)
        
        if processed_df.empty:
            return []
        
        # Convert to list of dictionaries
        matches_list = processed_df.to_dict(orient="records")
        
        return matches_list

    def get_recent_team_matches(self, team_name: str, league: str, num_matches: int = 7) -> pd.DataFrame:
        """
        Get recent matches for a specific team
        
        Args:
            team_name: Name of the team
            league: League name
            num_matches: Number of most recent matches to fetch
            
        Returns:
            DataFrame with the team's recent matches
        """
        logger.info(f"Fetching {num_matches} recent matches for {team_name} in {league}")
        
        # Get current season
        today = datetime.now().date()
        _, _, season_str = self.get_season_info(today.strftime("%Y-%m-%d"))
        
        # Get team URLs for this league and season
        team_info_list = self.get_team_urls(league, season_str)
        
        # Find the specific team
        team_info = None
        for info in team_info_list:
            if team_name.lower() in info["team_name"].lower():
                team_info = info
                break
        
        if not team_info:
            logger.warning(f"Team {team_name} not found in {league}")
            return pd.DataFrame()
        
        # Get match logs for this team
        matches_df = self.scrape_team_match_logs(team_info)
        
        if matches_df.empty:
            return pd.DataFrame()
        
        # Convert date column to datetime and sort by date
        if "Date" in matches_df.columns:
            matches_df["Date"] = pd.to_datetime(matches_df["Date"])
            matches_df = matches_df.sort_values("Date", ascending=False)
        
        # Take the most recent N matches
        recent_matches = matches_df.head(num_matches)
        
        # Process the DataFrame
        processed_df = self.process_match_dataframe(recent_matches)
        
        return processed_df
    
    def test_connection(self) -> bool:
        """
        Test connection to FBref
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            response = self.session.get("https://fbref.com/en/", timeout=10)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False