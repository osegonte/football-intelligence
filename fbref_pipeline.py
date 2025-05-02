#!/usr/bin/env python3
"""
FBref Match Statistics Pipeline

This script fetches historical match statistics for upcoming fixtures:
1. Reads upcoming matches from a SofaScore CSV file
2. For each match, fetches historical stats for both teams from FBref
3. Saves the data for analysis and prediction

Based on FBref scraping techniques.
"""
import time
import requests
import pandas as pd
import os
import sys
import argparse
import logging
from bs4 import BeautifulSoup, Comment
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fbref_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Define league IDs for common leagues
LEAGUE_IDS = {
    "Premier League": 9,
    "La Liga": 12,
    "Bundesliga": 20,
    "Serie A": 11,
    "Ligue 1": 13,
    "Champions League": 8,
    "Europa League": 19
}

def get_team_url(league_id, season_slug, team_name):
    """
    Get team URL from the league page with improved FBref compatibility
    
    Args:
        league_id: FBref league ID
        season_slug: Season slug (e.g., '2023-2024')
        team_name: Team to find
        
    Returns:
        Team URL or None if not found
    """
    logger.info(f"Looking for {team_name} in league ID {league_id}")
    
    # League name mapping
    league_names = {
        9: "Premier-League",
        12: "La-Liga",
        20: "Bundesliga",
        11: "Serie-A",
        13: "Ligue-1"
    }
    
    league_name_slug = league_names.get(league_id, "Premier-League")
    
    try:
        # Step 1: Fetch the league's main stats page
        base_url = f"https://fbref.com/en/comps/{league_id}/{league_name_slug}-Stats"
        logger.info(f"Fetching league base URL: {base_url}")
        
        resp = requests.get(base_url)
        if resp.status_code != 200:
            logger.warning(f"Failed to access {base_url}, status: {resp.status_code}")
            return None
            
        soup = BeautifulSoup(resp.text, "lxml")
        
        # Step 2: Find the season dropdown and get the correct season URL
        season_select = soup.find("select", id="season_select")
        if not season_select:
            season_select = soup.find("select", id="season")  # Alternative ID
            
        if not season_select:
            logger.warning("Could not find season dropdown on page")
            season_url = base_url  # Fallback to base URL
        else:
            # Find the target season option
            season_opt = None
            for option in season_select.find_all("option"):
                if season_slug in option.text:
                    season_opt = option
                    break
                    
            if not season_opt:
                logger.warning(f"Season {season_slug} not found in dropdown, using current season")
                season_url = base_url
            else:
                # Get the season-specific URL
                season_path = season_opt.get("value", "")
                if season_path.startswith("/"):
                    season_url = f"https://fbref.com{season_path}"
                else:
                    # Handle relative paths if needed
                    season_url = f"https://fbref.com/en/comps/{league_id}/{season_path}"
                    
        # Step 3: Fetch the season-specific page
        logger.info(f"Fetching season URL: {season_url}")
        resp2 = requests.get(season_url)
        if resp2.status_code != 200:
            logger.warning(f"Failed to access {season_url}, status: {resp2.status_code}")
            return None
            
        soup2 = BeautifulSoup(resp2.text, "lxml")
        
        # Step 4: Find the team stats table (often in HTML comments)
        from bs4 import Comment
        
        # Find the wrapper div
        wrapper_div = soup2.find("div", id="all_stats_squads_standard_for")
        
        if not wrapper_div:
            logger.warning("Could not find stats wrapper div, trying alternative methods")
            # Try to find any table with team links as fallback
            tables = soup2.find_all("table")
            for table in tables:
                links = table.select("tbody tr td a")
                if links and any("/squads/" in link.get("href", "") for link in links):
                    logger.info("Found alternative table with team links")
                    team_links = links
                    break
            else:
                logger.warning("No suitable tables found")
                return None
        else:
            # Extract from HTML comment
            comment = wrapper_div.find(string=lambda txt: isinstance(txt, Comment))
            
            if not comment:
                logger.warning("No commented table found in wrapper")
                return None
                
            # Parse the commented HTML
            cleaned = BeautifulSoup(comment, "lxml")
            table = cleaned.find("table", id="stats_squads_standard_for")
            
            if not table:
                logger.warning("No table found in comment")
                return None
                
            # Get team links
            team_links = table.select("tbody tr td a")
        
        # Step 5: Create mapping of team names to URLs
        team_map = {}
        for link in team_links:
            href = link.get("href", "")
            if "/squads/" in href:
                team_map[link.text.strip()] = f"https://fbref.com{href}"
        
        logger.info(f"Found {len(team_map)} teams")
        
        # Step 6: Find the requested team
        # Exact match
        if team_name in team_map:
            logger.info(f"Found exact match for {team_name}")
            return team_map[team_name]
        
        # Case-insensitive match
        for name, url in team_map.items():
            if team_name.lower() == name.lower():
                logger.info(f"Found case-insensitive match: '{name}' for '{team_name}'")
                return url
        
        # Partial match
        for name, url in team_map.items():
            if team_name.lower() in name.lower() or name.lower() in team_name.lower():
                logger.info(f"Found partial match: '{name}' for '{team_name}'")
                return url
                
        logger.warning(f"Team '{team_name}' not found in team list")
        return None
        
    except Exception as e:
        logger.error(f"Error finding team URL: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def scrape_team_matchlogs(team_url, num_matches=7, season_end_year="2025"):
    """
    Scrape match logs for a specific team
    
    Args:
        team_url: URL of the team page
        num_matches: Number of recent matches to return
        season_end_year: End year of the season
        
    Returns:
        DataFrame with match logs
    """
    # Get team name from URL
    team_name = team_url.split("/")[-1].replace("-Stats", "").replace("-", " ")
    
    # Extract team ID from URL
    team_id = team_url.split("/")[5]
    
    # Construct match logs URL
    logs_url = f"https://fbref.com/en/squads/{team_id}/matchlogs/{season_end_year}/summary/"
    
    logger.info(f"Fetching match logs for {team_name} from {logs_url}")
    
    try:
        # Get the page
        resp = requests.get(logs_url)
        soup = BeautifulSoup(resp.text, "lxml")
        
        # Find the match logs table
        tbl = soup.find("table", id=lambda x: x and "matchlogs" in x)
        
        if not tbl:
            logger.warning(f"No match logs table found for {team_name}")
            return pd.DataFrame()
        
        # Read the table
        df = pd.read_html(str(tbl))[0]
        
        # Add team information
        df["team"] = team_name
        
        # Clean up multi-index columns if present
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [' '.join(col).strip() for col in df.columns.values]
        
        # Sort by date and take the most recent N matches
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.sort_values("Date", ascending=False)
            df = df.head(num_matches)
        
        logger.info(f"Found {len(df)} matches for {team_name}")
        
        return df
        
    except Exception as e:
        logger.error(f"Error scraping match logs for {team_name}: {str(e)}")
        return pd.DataFrame()

def process_match_data(df):
    """
    Process and clean match data
    
    Args:
        df: Raw DataFrame from FBref
        
    Returns:
        Processed DataFrame
    """
    if df.empty:
        return df
    
    # Define column mappings
    column_mapping = {
        "Date": "date",
        "Comp": "competition",
        "Round": "round",
        "Venue": "venue",
        "Result": "result",
        "GF": "gf",
        "GA": "ga",
        "Opponent": "opponent",
        "xG": "xg",
        "xGA": "xga",
        "Poss": "possession",
        "Sh": "shots",
        "SoT": "shots_on_target",
        "FK": "free_kicks",
        "PK": "penalties",
        "PKatt": "penalty_attempts",
        "Crs": "corners_for"
    }
    
    # Select available columns and rename
    available_cols = [col for col in column_mapping.keys() if col in df.columns]
    df_clean = df[available_cols + ["team"]].copy()
    df_clean = df_clean.rename(columns={col: column_mapping[col] for col in available_cols})
    
    # Process date column
    if "date" in df_clean.columns and not pd.api.types.is_datetime64_dtype(df_clean["date"]):
        df_clean["date"] = pd.to_datetime(df_clean["date"])
    
    # Process possession
    if "possession" in df_clean.columns:
        try:
            df_clean["possession"] = df_clean["possession"].astype(str).str.rstrip("%").astype(float)
        except:
            pass
    
    # Process numeric columns
    numeric_cols = ["gf", "ga", "xg", "xga", "shots", "shots_on_target", "free_kicks", 
                   "penalties", "penalty_attempts", "corners_for"]
    
    for col in [c for c in numeric_cols if c in df_clean.columns]:
        try:
            df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce").fillna(0)
        except:
            pass
    
    # Process venue
    if "venue" in df_clean.columns:
        df_clean["venue"] = df_clean["venue"].apply(
            lambda x: "Home" if x == "Home" else "Away" if x == "Away" else x
        )
    
    # Add unique match_id
    try:
        df_clean["match_id"] = df_clean.apply(
            lambda row: f"{row['date'].strftime('%Y%m%d')}_{row['team']}_{row['opponent']}",
            axis=1
        )
    except:
        pass
    
    return df_clean

def load_upcoming_matches(csv_file):
    """
    Load upcoming matches from a CSV file
    
    Args:
        csv_file: Path to CSV file with upcoming matches
        
    Returns:
        DataFrame with upcoming matches
    """
    logger.info(f"Loading upcoming matches from {csv_file}")
    
    try:
        df = pd.read_csv(csv_file)
        
        # Check required columns
        required_cols = ['home_team', 'away_team']
        
        # Try alternate column names
        if not all(col in df.columns for col in required_cols):
            alternate_mapping = {
                'home': 'home_team',
                'away': 'away_team',
                'home_club': 'home_team',
                'away_club': 'away_team'
            }
            
            df = df.rename(columns={k: v for k, v in alternate_mapping.items() 
                                   if k in df.columns and v not in df.columns})
        
        # Check again
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            logger.error(f"CSV is missing required columns: {missing_cols}")
            return None
        
        # Ensure date column is present
        if 'date' not in df.columns:
            if 'match_date' in df.columns:
                df['date'] = df['match_date']
            else:
                df['date'] = datetime.now().strftime('%Y-%m-%d')
                logger.warning("No date column found, using current date")
        
        logger.info(f"Loaded {len(df)} upcoming matches")
        return df
        
    except Exception as e:
        logger.error(f"Error loading upcoming matches: {str(e)}")
        return None

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="FBref Match Statistics Pipeline")
    
    # Input and output options
    parser.add_argument('--input', required=True, help='CSV file with upcoming matches')
    parser.add_argument('--output-dir', default='match_analysis', help='Output directory')
    
    # Processing options
    parser.add_argument('--matches', type=int, default=7, help='Number of historical matches per team')
    parser.add_argument('--season', default='2024-2025', help='Season to use (YYYY-YYYY)')
    parser.add_argument('--delay', type=int, default=5, help='Delay between requests in seconds')
    parser.add_argument('--default-league', default='Premier League', help='Default league for teams')
    parser.add_argument('--limit', type=int, help='Limit number of fixtures to process')
    
    args = parser.parse_args()
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Extract season end year
    season_end_year = args.season.split('-')[1]
    
    # Load upcoming matches
    upcoming = load_upcoming_matches(args.input)
    
    if upcoming is None or len(upcoming) == 0:
        logger.error("No upcoming matches found")
        return 1
    
    # Process each fixture
    count = 0
    total_fixtures = len(upcoming)
    
    for idx, fixture in upcoming.iterrows():
        try:
            home_team = fixture['home_team']
            away_team = fixture['away_team']
            fixture_date = fixture.get('date', 'unknown')
            
            # Get league for each team
            home_league = fixture.get('home_league', args.default_league)
            away_league = fixture.get('away_league', args.default_league)
            
            logger.info(f"Processing fixture {count+1}/{total_fixtures}: {home_team} vs {away_team}")
            
            # Convert league names to IDs
            home_league_id = LEAGUE_IDS.get(home_league, LEAGUE_IDS[args.default_league])
            away_league_id = LEAGUE_IDS.get(away_league, LEAGUE_IDS[args.default_league])
            
            # Create fixture directory
            fixture_dir = os.path.join(args.output_dir, f"{home_team}_vs_{away_team}")
            os.makedirs(fixture_dir, exist_ok=True)
            
            # Get team URLs
            home_url = get_team_url(home_league_id, args.season, home_team)
            time.sleep(args.delay)  # Be polite
            
            away_url = get_team_url(away_league_id, args.season, away_team)
            time.sleep(args.delay)  # Be polite
            
            # Skip if either team URL is not found
            if not home_url:
                logger.warning(f"Could not find URL for {home_team}, skipping")
                continue
                
            if not away_url:
                logger.warning(f"Could not find URL for {away_team}, skipping")
                continue
            
            # Get historical matches
            home_df = scrape_team_matchlogs(home_url, args.matches, season_end_year)
            time.sleep(args.delay)  # Be polite
            
            away_df = scrape_team_matchlogs(away_url, args.matches, season_end_year)
            
            # Process match data
            home_clean = process_match_data(home_df)
            away_clean = process_match_data(away_df)
            
            # Add team role
            if not home_clean.empty:
                home_clean['team_role'] = 'home'
                home_clean.to_csv(os.path.join(fixture_dir, f"{home_team}_history.csv"), index=False)
                logger.info(f"Saved {len(home_clean)} matches for {home_team}")
            
            if not away_clean.empty:
                away_clean['team_role'] = 'away'
                away_clean.to_csv(os.path.join(fixture_dir, f"{away_team}_history.csv"), index=False)
                logger.info(f"Saved {len(away_clean)} matches for {away_team}")
            
            # Combine data
            if not home_clean.empty and not away_clean.empty:
                combined = pd.concat([home_clean, away_clean], ignore_index=True)
                combined.to_csv(os.path.join(fixture_dir, "combined_history.csv"), index=False)
                logger.info(f"Saved combined history with {len(combined)} matches")
            
            # Create fixture summary
            fixture_summary = {
                'fixture_date': fixture_date,
                'home_team': home_team,
                'away_team': away_team,
                'home_league': home_league,
                'away_league': away_league,
                'home_matches_found': len(home_clean),
                'away_matches_found': len(away_clean)
            }
            
            pd.DataFrame([fixture_summary]).to_csv(
                os.path.join(fixture_dir, "fixture_summary.csv"), index=False
            )
            
            # Increment counter
            count += 1
            
            # Check limit
            if args.limit and count >= args.limit:
                logger.info(f"Reached limit of {args.limit} fixtures")
                break
            
            # Delay before next fixture
            if idx < total_fixtures - 1:
                time.sleep(args.delay)
                
        except Exception as e:
            logger.error(f"Error processing fixture: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
    
    logger.info(f"Pipeline complete. Processed {count} fixtures")
    return 0

if __name__ == "__main__":
    sys.exit(main())