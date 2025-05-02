#!/usr/bin/env python3
"""
Test script for FBref scraper with improved path handling
"""
import sys
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Determine the project root and add it to Python path
script_dir = os.path.dirname(os.path.abspath(__file__))
# Print working directory and script directory for debugging
print(f"Current working directory: {os.getcwd()}")
print(f"Script directory: {script_dir}")

# Try different approaches to find the scrapers module
possible_paths = [
    script_dir,  # Current directory
    os.path.dirname(script_dir),  # Parent directory 
    os.path.join(script_dir, "football-intelligence"),  # In case we're in a different folder
]

for path in possible_paths:
    sys.path.insert(0, path)
    print(f"Added to Python path: {path}")

# Try importing with debug output for every path
import_success = False
import_error = None

# Check if the scrapers directory exists
for path in possible_paths:
    scrapers_dir = os.path.join(path, "scrapers")
    if os.path.isdir(scrapers_dir):
        print(f"Found scrapers directory at: {scrapers_dir}")
        # Check what files are in the directory
        files = os.listdir(scrapers_dir)
        print(f"Files in scrapers directory: {files}")

# Try direct import
try:
    import scrapers.fbref_scraper
    from scrapers.fbref_scraper import FBrefScraper
    logger.info("Successfully imported FBrefScraper directly")
    import_success = True
except ImportError as e:
    logger.error(f"Failed to import directly: {e}")
    import_error = e

if not import_success:
    # Try absolute import
    try:
        from football_intelligence.scrapers.fbref_scraper import FBrefScraper
        logger.info("Successfully imported FBrefScraper using absolute import")
        import_success = True
    except ImportError as e:
        logger.error(f"Failed to import using absolute path: {e}")
        import_error = e

if not import_success:
    logger.error(f"All import attempts failed: {import_error}")
    
    # Let's try a direct test without importing the module
    direct_test_only = True
else:
    direct_test_only = False

def test_scraper_directly():
    """Test the scraper by directly accessing and parsing match data"""
    print("\n=== Testing Direct Match Data Access ===\n")
    
    # Get Arsenal's team URL that we confirmed exists
    team_url = "https://fbref.com/en/squads/18bb7c10/Arsenal-Stats"
    
    # Extract team ID from URL
    team_id = team_url.split("/")[5]  # Should be 18bb7c10
    print(f"Team ID: {team_id}")
    
    # Construct match logs URL
    season_end_year = 2025  # Current year in your system
    match_logs_url = f"https://fbref.com/en/squads/{team_id}/matchlogs/{season_end_year}/summary/"
    print(f"Match logs URL: {match_logs_url}")
    
    try:
        import requests
        from bs4 import BeautifulSoup
        import pandas as pd
        
        # Add proper headers to avoid being blocked
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://fbref.com/',
            'Connection': 'keep-alive'
        }
        
        # Request the match logs page
        print("Requesting match logs page...")
        response = requests.get(match_logs_url, headers=headers)
        print(f"Status code: {response.status_code}")
        
        if response.status_code == 200:
            print("Successfully accessed match logs page")
            
            # Find the match logs table
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Look for tables with "matchlogs" in the ID
            matchlog_tables = soup.find_all("table", id=lambda x: x and "matchlogs" in str(x))
            print(f"Found {len(matchlog_tables)} matchlog tables")
            
            if matchlog_tables:
                # Try to read the first table into a DataFrame
                try:
                    print("Attempting to read table into DataFrame...")
                    df = pd.read_html(str(matchlog_tables[0]))[0]
                    print(f"Successfully read table with {len(df)} rows")
                    
                    # Display column names
                    print(f"Columns: {df.columns.tolist()}")
                    
                    # Display first few rows
                    print("\nFirst 3 matches:")
                    for i, (_, row) in enumerate(df.head(3).iterrows(), 1):
                        date = row.get('Date', 'Unknown date')
                        opponent = row.get('Opponent', 'Unknown opponent')
                        venue = row.get('Venue', '')
                        result = row.get('Result', '')
                        
                        score = "?"
                        if 'GF' in df.columns and 'GA' in df.columns:
                            score = f"{row.get('GF', '?')}-{row.get('GA', '?')}"
                        
                        print(f"{i}. {date} - Arsenal vs {opponent} ({venue}) - {result} {score}")
                    
                except Exception as e:
                    print(f"Error reading table into DataFrame: {e}")
                    import traceback
                    traceback.print_exc()
            else:
                print("No matchlog tables found on the page")
                
                # Save HTML for inspection
                debug_file = "fbref_debug.html"
                with open(debug_file, "w", encoding="utf-8") as f:
                    f.write(response.text)
                print(f"Saved HTML to {debug_file} for debugging")
        else:
            print(f"Failed to access match logs page: {response.status_code}")
    
    except Exception as e:
        print(f"Error in direct testing: {e}")
        import traceback
        traceback.print_exc()

def test_fbref_scraper():
    """Test the FBrefScraper class"""
    if direct_test_only:
        print("\nSkipping FBrefScraper test due to import issues")
        return
        
    print("\n=== Testing FBrefScraper Class ===\n")
    
    # Create scraper instance
    logger.info("Creating FBrefScraper instance")
    scraper = FBrefScraper()
    
    # Test parameters
    team_name = "Arsenal"
    league_name = "Premier League"
    num_matches = 3
    
    print(f"Attempting to fetch {num_matches} matches for {team_name} in {league_name}")
    
    # Get team matches
    try:
        logger.info(f"Getting recent matches for {team_name}")
        matches_df = scraper.get_recent_team_matches(team_name, league_name, num_matches)
        
        if matches_df is None:
            logger.error("get_recent_team_matches returned None")
            return
            
        if matches_df.empty:
            logger.warning(f"No matches found for {team_name}")
            return
        
        # Display match count
        print(f"Found {len(matches_df)} matches")
        
        # Display match information
        print("\nMatch Details:")
        for i, (_, match) in enumerate(matches_df.iterrows(), 1):
            date = match.get('Date', 'Unknown date')
            opponent = match.get('Opponent', 'Unknown opponent')
            venue = match.get('Venue', '')
            result = match.get('Result', '')
            score = f"{match.get('GF', 0)}-{match.get('GA', 0)}"
            
            print(f"{i}. {date} - {team_name} vs {opponent} ({venue}) - {result} {score}")
            
        print("\nTest completed successfully")
        
    except Exception as e:
        logger.error(f"Error using FBrefScraper: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # First test direct access
    test_scraper_directly()
    
    # Then test the scraper class if imports worked
    test_fbref_scraper()