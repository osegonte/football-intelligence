#!/usr/bin/env python3
"""
FBref Scraper for Reliable Match Statistics

This scraper fetches only reliable basic match data:
- Date, teams, venue, result, goals
- Sets defaults for advanced stats to maintain database compatibility
"""
import time
import requests
import pandas as pd
import logging
from bs4 import BeautifulSoup
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FBrefScraper:
    """
    FBref scraper focusing on reliable match statistics
    """
    
    def __init__(self):
        self.base_url = "https://fbref.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://fbref.com/',
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # Competition IDs for FBref URLs
        self.competition_ids = {
            "Premier League": 9,
            "La Liga": 12,
            "Bundesliga": 20,
            "Serie A": 11,
            "Ligue 1": 13,
            "Champions League": 8,
            "Europa League": 19,
            "Championship": 10,
            "League One": 15,
            "League Two": 16
        }

    def get_season_info(self, date_str=None):
        """Get season information based on date"""
        if date_str:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
        else:
            date_obj = datetime.now().date()
        
        # European seasons run from August to May
        if date_obj.month >= 8:  
            season_start = date_obj.year
        else:
            season_start = date_obj.year - 1
            
        season_end = season_start + 1
        season_str = f"{season_start}-{season_end}"
        
        return season_start, season_end, season_str

    def get_team_url(self, team_name, league_name="Premier League"):
        """Get FBref URL for a team"""
        logger.info(f"Getting URL for {team_name} in {league_name}")
        
        league_id = self.competition_ids.get(league_name)
        if not league_id:
            # Try to find a partial match
            for league, id in self.competition_ids.items():
                if league_name.lower() in league.lower() or league.lower() in league_name.lower():
                    league_id = id
                    break
        
        if not league_id:
            logger.error(f"League ID not found for {league_name}")
            return None
        
        _, _, season_str = self.get_season_info()
        league_url = f"{self.base_url}/en/comps/{league_id}/{season_str}/{season_str}-Stats"
        
        try:
            response = self.session.get(league_url, timeout=30)
            if response.status_code != 200:
                logger.error(f"Failed to access league page: {response.status_code}")
                return None
            
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Find team links
            team_links = {}
            tables = soup.find_all("table")
            
            for table in tables:
                links = table.select("tbody tr td a")
                for link in links:
                    href = link.get("href", "")
                    if "/squads/" in href:
                        team_links[link.text.strip()] = f"{self.base_url}{href}"
            
            # Search for team (exact, case-insensitive, or partial match)
            for name, url in team_links.items():
                if team_name.lower() == name.lower():
                    logger.info(f"Found team URL: {url}")
                    return url
            
            # Partial match
            for name, url in team_links.items():
                if team_name.lower() in name.lower() or name.lower() in team_name.lower():
                    logger.info(f"Found team URL (partial match): {url}")
                    return url
            
            logger.error(f"Team {team_name} not found in {league_name}")
            return None
            
        except Exception as e:
            logger.error(f"Error getting team URL: {str(e)}")
            return None

    def get_recent_team_matches(self, team_name, league_name="Premier League", num_matches=7):
        """Get recent matches for a team with reliable statistics only"""
        logger.info(f"Fetching matches for {team_name} in {league_name}")
        
        # Get team URL
        team_url = self.get_team_url(team_name, league_name)
        if not team_url:
            return pd.DataFrame()
        
        # Extract team ID from URL
        try:
            team_id = team_url.split("/")[5]
        except:
            logger.error(f"Could not extract team ID from URL: {team_url}")
            return pd.DataFrame()
        
        # Get current season year
        current_year = datetime.now().year
        
        # Construct match logs URL
        logs_url = f"{self.base_url}/en/squads/{team_id}/matchlogs/{current_year}/summary/"
        logger.info(f"Fetching matches from: {logs_url}")
        
        try:
            # Add delay to be respectful
            time.sleep(3)
            
            response = self.session.get(logs_url, timeout=30)
            if response.status_code != 200:
                logger.error(f"Failed to access match logs: {response.status_code}")
                return pd.DataFrame()
            
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Find match logs table
            table = soup.find("table", id=lambda x: x and "matchlogs" in x)
            
            if not table:
                logger.error("No match logs table found")
                return pd.DataFrame()
            
            # Read table into DataFrame
            df = pd.read_html(str(table))[0]
            
            # Handle multi-level columns
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [' '.join(col).strip() for col in df.columns.values]
            
            # Filter for completed matches only
            if "Result" in df.columns:
                df = df[df["Result"].notna() & (df["Result"] != "")]
            
            # Sort by date (most recent first)
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"], errors='coerce')
                df = df.sort_values("Date", ascending=False)
            
            # Limit to requested number of matches
            df = df.head(num_matches)
            
            # Process matches to create standardized format
            processed_matches = []
            
            for _, row in df.iterrows():
                try:
                    # Extract reliable data
                    match_date = pd.to_datetime(row.get('Date', ''), errors='coerce')
                    if pd.isna(match_date):
                        continue
                    
                    match_data = {
                        'match_id': f"{match_date.strftime('%Y%m%d')}_{team_name}_{row.get('Opponent', '').strip()}",
                        'date': match_date,
                        'team': team_name,
                        'opponent': row.get('Opponent', '').strip(),
                        'venue': row.get('Venue', ''),
                        'result': row.get('Result', ''),
                        'comp': row.get('Comp', league_name),
                        'round': row.get('Round', ''),
                        'gf': int(row.get('GF', 0)) if pd.notna(row.get('GF')) else 0,
                        'ga': int(row.get('GA', 0)) if pd.notna(row.get('GA')) else 0,
                        
                        # Set defaults for unreliable advanced stats
                        'xg': 0.0,
                        'xga': 0.0,
                        'sh': 0,
                        'sot': 0,
                        'dist': 0.0,
                        'fk': 0,
                        'pk': 0,
                        'pkatt': 0,
                        'possession': 0.0,
                        'yellow_cards': 0,
                        'red_cards': 0,
                        'fouls': 0,
                        'corners': 0,
                        'opp_corners': 0,
                        
                        # Extract season from date
                        'season': f"{match_date.year-1}/{str(match_date.year)[2:]}" if match_date.month < 8 else f"{match_date.year}/{str(match_date.year+1)[2:]}"
                    }
                    
                    processed_matches.append(match_data)
                    
                except Exception as e:
                    logger.warning(f"Error processing match row: {e}")
                    continue
            
            result_df = pd.DataFrame(processed_matches)
            logger.info(f"Successfully fetched {len(result_df)} matches for {team_name}")
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error fetching matches: {str(e)}")
            return pd.DataFrame()

    def fetch_matches_for_date(self, date_str, league_filter=None):
        """Fetch matches for a specific date"""
        logger.info(f"Fetching matches for date: {date_str}")
        
        # This is a placeholder - FBref doesn't have easy date-based endpoints
        # You would need to implement logic to find matches for specific dates
        
        # For now, return empty DataFrame
        return pd.DataFrame()