#!/usr/bin/env python3
"""
FBref Database Updater

This script uses the FBref scraper to fetch match data and store it in the database.
It can update data for specific teams, leagues, or upcoming fixtures.

Usage:
  python fbref_db_updater.py --team "Arsenal" --matches 7
  python fbref_db_updater.py --fixture "Arsenal" "Tottenham"
  python fbref_db_updater.py --league "Premier League" --limit 5
  python fbref_db_updater.py --upcoming-fixtures --days 14
"""
import os
import sys
import argparse
import logging
import time
from datetime import datetime, timedelta
import pandas as pd
import random

# Import the database connector
from data_processing.db_connector import FootballDBConnector

# Add parent directory to path for importing the FBref scraper
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
sys.path.append(parent_dir)

# Import the FBref scraper
from scrapers.fbref_scraper import FBrefScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fbref_db_updater.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FBrefDatabaseUpdater:
    """
    Updates the football database with data from FBref.
    """
    
    def __init__(self, delay_min=3, delay_max=5):
        """
        Initialize the updater.
        
        Args:
            delay_min: Minimum delay between requests in seconds
            delay_max: Maximum delay between requests in seconds
        """
        # Initialize database connector
        self.db = FootballDBConnector()
        
        # Initialize FBref scraper
        self.fbref = FBrefScraper()
        
        # Set delays for rate limiting
        self.delay_min = delay_min
        self.delay_max = delay_max
    
    def add_delay(self):
        """Add a random delay between requests to avoid rate limiting"""
        delay = self.delay_min + random.random() * (self.delay_max - self.delay_min)
        time.sleep(delay)
    
    def update_team(self, team_name, league_name="Premier League", num_matches=7):
        """
        Update match data for a specific team.
        
        Args:
            team_name: Name of the team
            league_name: Name of the league
            num_matches: Number of recent matches to fetch
            
        Returns:
            Number of matches stored in the database
        """
        logger.info(f"Updating data for {team_name} ({league_name})")
        
        # Fetch recent matches for the team
        matches_df = self.fbref.get_recent_team_matches(team_name, league_name, num_matches)
        
        if matches_df.empty:
            logger.warning(f"No matches found for {team_name}")
            return 0
        
        # Store matches in the database
        match_count = self.db.store_matches_from_dataframe(matches_df)
        
        logger.info(f"Stored {match_count} matches for {team_name} in the database")
        return match_count
    
    def update_fixture(self, home_team, away_team, league_name="Premier League", num_matches=7):
        """
        Update match data for a specific fixture.
        
        Args:
            home_team: Name of the home team
            away_team: Name of the away team
            league_name: Name of the league
            num_matches: Number of recent matches to fetch for each team
            
        Returns:
            Number of matches stored in the database
        """
        logger.info(f"Updating data for fixture: {home_team} vs {away_team}")
        
        # Update home team data
        home_count = self.update_team(home_team, league_name, num_matches)
        
        # Add delay to avoid rate limiting
        self.add_delay()
        
        # Update away team data
        away_count = self.update_team(away_team, league_name, num_matches)
        
        return home_count + away_count
    
    def update_league_teams(self, league_name="Premier League", limit=None):
        """
        Update match data for teams in a specific league.
        
        Args:
            league_name: Name of the league
            limit: Maximum number of teams to update
            
        Returns:
            Number of matches stored in the database
        """
        logger.info(f"Updating data for league: {league_name}")
        
        # Get team URLs for the league
        _, _, season_str = self.fbref.get_season_info()
        
        # Look up the league ID
        league_id = None
        for league, id in self.fbref.competition_ids.items():
            if league.lower() == league_name.lower():
                league_id = id
                break
        
        if not league_id:
            # Try to find a partial match
            for league, id in self.fbref.competition_ids.items():
                if league_name.lower() in league.lower() or league.lower() in league_name.lower():
                    league_id = id
                    break
        
        if not league_id:
            logger.error(f"Unknown league: {league_name}")
            return 0
        
        # Construct the URL for the league standings page
        base_url = f"https://fbref.com/en/comps/{league_id}/{season_str}/{season_str}-Stats"
        
        # Fetch the league page
        try:
            # Make a request to the league page
            response = self.fbref.session.get(base_url, timeout=30)
            if response.status_code != 200:
                logger.error(f"Failed to fetch league page: {base_url}")
                return 0
            
            # Parse the HTML to extract team names and URLs
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.text, "lxml")
            
            # Find the table with team standings
            tables = soup.find_all("table")
            teams = []
            
            for table in tables:
                # Check if this is a standings table
                if table.find("th", string="Squad"):
                    # Extract team names from the table
                    for row in table.find_all("tr")[1:]:  # Skip header row
                        squad_cell = row.find("td", {"data-stat": "squad"})
                        if squad_cell and squad_cell.a:
                            team_name = squad_cell.a.text.strip()
                            teams.append(team_name)
            
            if not teams:
                logger.warning(f"No teams found for league: {league_name}")
                return 0
            
            logger.info(f"Found {len(teams)} teams in {league_name}")
            
            # Limit the number of teams if specified
            if limit and limit < len(teams):
                teams = teams[:limit]
                logger.info(f"Limiting update to {limit} teams")
            
            # Update data for each team
            total_matches = 0
            
            for i, team_name in enumerate(teams):
                logger.info(f"Updating team {i+1}/{len(teams)}: {team_name}")
                
                # Update team data
                match_count = self.update_team(team_name, league_name)
                total_matches += match_count
                
                # Add delay to avoid rate limiting (except for the last team)
                if i < len(teams) - 1:
                    self.add_delay()
            
            logger.info(f"Updated {total_matches} matches for {len(teams)} teams in {league_name}")
            return total_matches
            
        except Exception as e:
            logger.error(f"Error updating league teams: {str(e)}")
            return 0
    
    def update_upcoming_fixtures(self, days=7, limit=None):
        """
        Update data for teams in upcoming fixtures.
        
        Args:
            days: Number of days to look ahead
            limit: Maximum number of fixtures to update
            
        Returns:
            Number of matches stored in the database
        """
        logger.info(f"Updating data for upcoming fixtures in the next {days} days")
        
        # Get upcoming fixtures from the database
        fixtures = self.db.get_upcoming_fixtures(days=days)
        
        if not fixtures:
            logger.warning("No upcoming fixtures found")
            return 0
        
        logger.info(f"Found {len(fixtures)} upcoming fixtures")
        
        # Limit the number of fixtures if specified
        if limit and limit < len(fixtures):
            fixtures = fixtures[:limit]
            logger.info(f"Limiting update to {limit} fixtures")
        
        # Update data for each fixture
        total_matches = 0
        
        for i, fixture in enumerate(fixtures):
            home_team = fixture['home_team']
            away_team = fixture['away_team']
            competition = fixture['competition']
            
            logger.info(f"Updating fixture {i+1}/{len(fixtures)}: {home_team} vs {away_team} ({competition})")
            
            # Update fixture data
            match_count = self.update_fixture(home_team, away_team, competition)
            total_matches += match_count
            
            # Add delay to avoid rate limiting (except for the last fixture)
            if i < len(fixtures) - 1:
                self.add_delay()
        
        logger.info(f"Updated {total_matches} matches for {len(fixtures)} upcoming fixtures")
        return total_matches
    
    def show_database_stats(self):
        """Show statistics about the database"""
        stats = self.db.get_database_statistics()
        
        if not stats:
            logger.warning("Unable to retrieve database statistics")
            return
        
        print("\nDatabase Statistics:")
        print(f"Teams: {stats.get('team_count', 0):,}")
        print(f"Competitions: {stats.get('competition_count', 0):,}")
        print(f"Matches: {stats.get('match_count', 0):,}")
        print(f"Seasons: {stats.get('season_count', 0):,}")
        
        if 'first_match_date' in stats and 'last_match_date' in stats:
            print(f"Date range: {stats['first_match_date']} to {stats['last_match_date']}")
        
        if 'top_competitions' in stats:
            print("\nTop Competitions:")
            for i, comp in enumerate(stats['top_competitions'], 1):
                print(f"  {i}. {comp['name']} ({comp['match_count']:,} matches)")
        
        if 'top_teams' in stats:
            print("\nTop Teams by Match Count:")
            for i, team in enumerate(stats['top_teams'], 1):
                print(f"  {i}. {team['name']} ({team['match_count']:,} matches)")

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Update the football database with data from FBref")
    
    # Target options (mutually exclusive)
    target_group = parser.add_mutually_exclusive_group(required=True)
    target_group.add_argument('--team', type=str, help='Update data for a specific team')
    target_group.add_argument('--fixture', type=str, nargs=2, metavar=('HOME', 'AWAY'), help='Update data for a specific fixture')
    target_group.add_argument('--league', type=str, help='Update data for teams in a specific league')
    target_group.add_argument('--upcoming-fixtures', action='store_true', help='Update data for teams in upcoming fixtures')
    target_group.add_argument('--stats', action='store_true', help='Show database statistics')
    
    # Additional options
    parser.add_argument('--league-name', type=str, default='Premier League', help='League name for the team or fixture')
    parser.add_argument('--matches', type=int, default=7, help='Number of matches to fetch per team')
    parser.add_argument('--limit', type=int, help='Maximum number of teams or fixtures to update')
    parser.add_argument('--days', type=int, default=7, help='Number of days to look ahead for upcoming fixtures')
    parser.add_argument('--delay-min', type=float, default=3, help='Minimum delay between requests in seconds')
    parser.add_argument('--delay-max', type=float, default=5, help='Maximum delay between requests in seconds')
    
    args = parser.parse_args()
    
    # Initialize the updater
    updater = FBrefDatabaseUpdater(args.delay_min, args.delay_max)
    
    # Perform the requested action
    if args.team:
        updater.update_team(args.team, args.league_name, args.matches)
    
    elif args.fixture:
        updater.update_fixture(args.fixture[0], args.fixture[1], args.league_name, args.matches)
    
    elif args.league:
        updater.update_league_teams(args.league, args.limit)
    
    elif args.upcoming_fixtures:
        updater.update_upcoming_fixtures(args.days, args.limit)
    
    elif args.stats:
        updater.show_database_stats()
    
    return 0

if __name__ == '__main__':
    sys.exit(main())