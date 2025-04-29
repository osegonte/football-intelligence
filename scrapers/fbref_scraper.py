import time
import re
import os
from datetime import datetime
import pandas as pd
import logging
import random
import requests
from scrapers.scraper_utils import (
    add_random_delay,
    get_random_headers,
    standardize_match_data
)

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
    Scraper for FBref football match data, used as a fallback for SofaScore
    """
    
    def __init__(self):
        """Initialize the FBref scraper"""
        # List of major leagues to fetch - can be expanded
        self.leagues = [
            {"name": "Premier League", "short_name": "EPL", "country": "England", 
             "url_suffix": "Premier-League"},
            {"name": "La Liga", "short_name": "La Liga", "country": "Spain", 
             "url_suffix": "La-Liga"},
            {"name": "Bundesliga", "short_name": "Bundesliga", "country": "Germany", 
             "url_suffix": "Bundesliga"},
            {"name": "Serie A", "short_name": "Serie A", "country": "Italy", 
             "url_suffix": "Serie-A"},
            {"name": "Ligue 1", "short_name": "Ligue 1", "country": "France", 
             "url_suffix": "Ligue-1"},
            {"name": "Champions League", "short_name": "UCL", "country": "Europe", 
             "url_suffix": "Champions-League"},
            {"name": "Europa League", "short_name": "UEL", "country": "Europe", 
             "url_suffix": "Europa-League"},
            {"name": "Premier League", "short_name": "EPL", "country": "England", 
             "url_suffix": "Premier-League"}
        ]
        
        # Create a session for requests
        self.session = requests.Session()
        
        # Configure headers
        self.session.headers.update(get_random_headers())
        self.session.headers['Referer'] = 'https://fbref.com/'
        
        # Rate limiting parameters
        self.min_request_delay = 3  # seconds
        self.max_request_delay = 7  # seconds
    
    def get_season_for_date(self, date_str):
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
    
    def build_league_url(self, league, season_str):
        """
        Build the URL for a specific league's fixtures
        
        Args:
            league: Dictionary containing league information
            season_str: Season string in format "YYYY-YYYY"
            
        Returns:
            URL string for the league's fixtures page
        """
        comp_id = self._get_competition_id(league["name"])
        return f"https://fbref.com/en/comps/{comp_id}/{season_str}/schedule/{league['url_suffix']}-Scores-and-Fixtures"
    
    def _get_competition_id(self, league_name):
        """
        Get the competition ID used in FBref URLs
        
        Args:
            league_name: Name of the league
            
        Returns:
            Competition ID string
        """
        # Map of league names to their FBref competition IDs
        comp_ids = {
            "Premier League": "9",
            "La Liga": "12",
            "Bundesliga": "20",
            "Serie A": "11",
            "Ligue 1": "13",
            "Champions League": "8",
            "Europa League": "19"
        }
        
        return comp_ids.get(league_name, "9")  # Default to Premier League ID if not found
    
    def fetch_matches_for_date(self, target_date):
        """
        Fetch all matches for a specific date from FBref
        
        Args:
            target_date: Date string in format YYYY-MM-DD
            
        Returns:
            List of match dictionaries or empty list if failed
        """
        logger.info(f"Fetching matches from FBref for {target_date}")
        
        # Get season information for the target date
        _, _, season_str = self.get_season_for_date(target_date)
        
        # Format the target date for comparison
        target_date_obj = datetime.strptime(target_date, "%Y-%m-%d").date()
        
        all_matches = []
        
        for league in self.leagues:
            try:
                league_url = self.build_league_url(league, season_str)
                logger.info(f"Fetching {league['name']} schedule from {league_url}")
                
                # Add delay to avoid rate limiting
                add_random_delay(self.min_request_delay, self.max_request_delay)
                
                # Request the page
                response = self.session.get(league_url, timeout=30)
                
                if response.status_code != 200:
                    logger.warning(f"Failed to fetch {league['name']} schedule: HTTP {response.status_code}")
                    continue
                
                # Use pandas to read the HTML table
                try:
                    tables = pd.read_html(response.text)
                    
                    if not tables or len(tables) == 0:
                        logger.warning(f"No tables found for {league['name']}")
                        continue
                    
                    # Get the schedule table (usually the first one)
                    schedule_df = tables[0]
                    
                    # Find matches for the target date
                    if 'Date' in schedule_df.columns:
                        for index, row in schedule_df.iterrows():
                            try:
                                # Parse the date from FBref format
                                date_str = str(row['Date'])
                                if pd.isna(date_str) or date_str == 'nan':
                                    continue
                                
                                # FBref date format is typically 'YYYY-MM-DD'
                                if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
                                    match_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                                elif re.match(r'\d{2}/\d{2}/\d{4}', date_str):
                                    match_date = datetime.strptime(date_str, "%m/%d/%Y").date()
                                else:
                                    continue
                                
                                # Check if the match is on the target date
                                if match_date == target_date_obj:
                                    # Extract match details
                                    home_team = row.get('Home', 'Unknown')
                                    away_team = row.get('Away', 'Unknown')
                                    
                                    # Extract time if available
                                    time_str = row.get('Time', '')
                                    if pd.isna(time_str):
                                        time_str = '00:00'
                                    
                                    # Create match object
                                    match = {
                                        'id': f"fbref_{league['short_name']}_{index}",
                                        'home_team': home_team,
                                        'away_team': away_team,
                                        'league': league['name'],
                                        'country': league['country'],
                                        'start_time': time_str,
                                        'date': target_date,
                                        'source': 'fbref'
                                    }
                                    
                                    all_matches.append(match)
                            except Exception as row_error:
                                logger.warning(f"Error processing row: {str(row_error)}")
                                continue
                    else:
                        logger.warning(f"No 'Date' column found in {league['name']} schedule")
                        
                except Exception as parse_error:
                    logger.error(f"Error parsing HTML for {league['name']}: {str(parse_error)}")
                    continue
                    
            except Exception as league_error:
                logger.error(f"Error fetching {league['name']}: {str(league_error)}")
                continue
        
        if all_matches:
            logger.info(f"Found {len(all_matches)} matches from FBref for {target_date}")
            return standardize_match_data(all_matches, target_date)
        else:
            logger.warning(f"No matches found from FBref for {target_date}")
            return []
    
    def fetch_match_details(self, match_url):
        """
        Fetch detailed information for a specific match
        
        Args:
            match_url: URL to the match page on FBref
            
        Returns:
            Dictionary with detailed match information or None if failed
        """
        try:
            logger.info(f"Fetching match details from {match_url}")
            
            # Add delay to avoid rate limiting
            add_random_delay(self.min_request_delay, self.max_request_delay)
            
            # Request the page
            response = self.session.get(match_url, timeout=30)
            
            if response.status_code != 200:
                logger.warning(f"Failed to fetch match details: HTTP {response.status_code}")
                return None
            
            # Use pandas to read the HTML tables
            tables = pd.read_html(response.text)
            
            # Extract match details from tables
            match_details = {}
            
            # Typically, tables include: 
            # - Match Summary
            # - Team Stats
            # - Player Stats for each team
            
            # Basic parsing - in a real implementation, this would be more comprehensive
            if len(tables) > 0:
                # Get summary table
                summary_df = tables[0]
                
                # Extract relevant information
                # This is a simplified version - full implementation would be more detailed
                match_details['summary'] = summary_df.to_dict(orient='records')
                
                if len(tables) > 1:
                    match_details['team_stats'] = tables[1].to_dict(orient='records')
                
                return match_details
            
            return None
            
        except Exception as e:
            logger.error(f"Error fetching match details: {str(e)}")
            return None