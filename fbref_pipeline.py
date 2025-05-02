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
    
    # Try different URL patterns and seasons
    url_patterns = [
        # Current season
        f"https://fbref.com/en/comps/{league_id}/{season_slug}/{season_slug}-Stats",
        # Alternative URL format
        f"https://fbref.com/en/comps/{league_id}/stats/{season_slug}-Stats",
        # Try previous season as fallback
        f"https://fbref.com/en/comps/{league_id}/2022-2023/2022-2023-Stats",
    ]
    
    for base_url in url_patterns:
        try:
            logger.info(f"Trying URL: {base_url}")
            
            # Add proper headers to avoid being blocked
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': 'https://fbref.com/',
                'Connection': 'keep-alive'
            }
            
            resp = requests.get(base_url, headers=headers)
            
            if resp.status_code != 200:
                logger.warning(f"Failed to access {base_url}, status: {resp.status_code}")
                continue
                
            soup = BeautifulSoup(resp.text, "lxml")
            
            # Try to find the table in different ways
            
            # 1. First, look for the wrapper div
            wrapper_div = soup.find("div", id="all_stats_squads_standard_for")
            
            if wrapper_div:
                # Try to extract from HTML comment
                from bs4 import Comment
                comment = wrapper_div.find(string=lambda txt: isinstance(txt, Comment))
                
                if comment:
                    # Parse the commented HTML
                    comment_soup = BeautifulSoup(comment, "lxml")
                    table = comment_soup.find("table", id="stats_squads_standard_for")
                    
                    if table:
                        team_links = table.select("tbody tr td a")
                        if team_links:
                            logger.info(f"Found team links in commented table")
                        else:
                            logger.warning("No team links found in commented table")
                            continue
                    else:
                        logger.warning("No table found in comment")
                        continue
                else:
                    logger.warning("No commented table found in wrapper")
                    continue
            else:
                # 2. If no wrapper div, try to find the table directly
                table = soup.find("table", id="stats_squads_standard_for")
                
                if not table:
                    # 3. Try any table with team links as fallback
                    tables = soup.find_all("table")
                    for t in tables:
                        links = t.select("tbody tr td a")
                        if links and any("/squads/" in link.get("href", "") for link in links):
                            logger.info("Found alternative table with team links")
                            team_links = links
                            break
                    else:
                        logger.warning("No suitable tables found")
                        continue
                else:
                    team_links = table.select("tbody tr td a")
            
            # Create mapping of team names to URLs
            team_map = {}
            for link in team_links:
                href = link.get("href", "")
                if "/squads/" in href:
                    team_map[link.text.strip()] = f"https://fbref.com{href}"
            
            logger.info(f"Found {len(team_map)} teams")
            
            # Try to find the requested team
            # 1. Exact match
            if team_name in team_map:
                logger.info(f"Found exact match for {team_name}")
                return team_map[team_name]
            
            # 2. Case-insensitive match
            for name, url in team_map.items():
                if team_name.lower() == name.lower():
                    logger.info(f"Found case-insensitive match: '{name}' for '{team_name}'")
                    return url
            
            # 3. Partial match
            for name, url in team_map.items():
                if team_name.lower() in name.lower() or name.lower() in team_name.lower():
                    logger.info(f"Found partial match: '{name}' for '{team_name}'")
                    return url
                    
            logger.warning(f"Team '{team_name}' not found in team list")
            
        except Exception as e:
            logger.error(f"Error finding team URL: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
    
    return None

def scrape_team_matchlogs(team_url, num_matches=7, season_end_year="2025"):
    """
    Scrape match logs for a specific team's most recent matches
    
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
    
    # Construct match logs URL for current season
    logs_url = f"https://fbref.com/en/squads/{team_id}/matchlogs/all_comps/summary/"
    
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
        
        # Sort by date and filter for completed matches only (those with a result)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.sort_values("Date", ascending=False)
            
        # Only include matches that have a result (not future fixtures)
        if "Result" in df.columns:
            df = df[df["Result"].notna() & (df["Result"] != "")]
        
        # Take only the specified number of most recent matches
        df = df.head(num_matches)
        
        # If we didn't get enough matches, try to get matches from previous season
        if len(df) < num_matches:
            prev_logs_url = f"https://fbref.com/en/squads/{team_id}/{season_end_year-1}-{season_end_year}/matchlogs/all_comps/summary/"
            logger.info(f"Not enough matches, trying previous season: {prev_logs_url}")
            
            try:
                prev_resp = requests.get(prev_logs_url)
                prev_soup = BeautifulSoup(prev_resp.text, "lxml")
                prev_tbl = prev_soup.find("table", id=lambda x: x and "matchlogs" in x)
                
                if prev_tbl:
                    prev_df = pd.read_html(str(prev_tbl))[0]
                    prev_df["team"] = team_name
                    
                    # Clean up multi-index columns
                    if isinstance(prev_df.columns, pd.MultiIndex):
                        prev_df.columns = [' '.join(col).strip() for col in prev_df.columns.values]
                    
                    # Convert date and filter completed matches
                    if "Date" in prev_df.columns:
                        prev_df["Date"] = pd.to_datetime(prev_df["Date"])
                        prev_df = prev_df.sort_values("Date", ascending=False)
                    
                    if "Result" in prev_df.columns:
                        prev_df = prev_df[prev_df["Result"].notna() & (prev_df["Result"] != "")]
                    
                    # Combine and take the top N matches
                    df = pd.concat([df, prev_df], ignore_index=True)
                    df = df.sort_values("Date", ascending=False)
                    df = df.head(num_matches)
            except Exception as e:
                logger.warning(f"Error fetching previous season: {str(e)}")
        
        logger.info(f"Found {len(df)} completed matches for {team_name}")
        
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


def validate_match_data(csv_file):
    """
    Validate that match data contains only completed matches with statistics
    
    Args:
        csv_file: Path to the CSV file to validate
    """
    print(f"\nValidating {csv_file}:")
    try:
        df = pd.read_csv(csv_file)
        print(f"- Total rows: {len(df)}")
        
        # Check for completed matches (with results)
        if 'result' in df.columns:
            complete = df['result'].notna() & (df['result'] != '')
            print(f"- Completed matches: {complete.sum()} of {len(df)}")
        
        # Check for non-zero statistics
        if 'gf' in df.columns and 'ga' in df.columns:
            has_goals = (df['gf'] > 0) | (df['ga'] > 0)
            print(f"- Matches with goals: {has_goals.sum()} of {len(df)}")
        
        # Check date range
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            print(f"- Date range: {df['date'].min()} to {df['date'].max()}")
            
            # Check for future fixtures
            today = pd.Timestamp.now()
            future = df[df['date'] > today]
            if len(future) > 0:
                print(f"- WARNING: {len(future)} future fixtures detected")
                print(future[['date', 'team', 'opponent', 'result']].head())
        
    except Exception as e:
        print(f"Error validating CSV: {str(e)}")

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
    try:
        season_end_year = int(args.season.split('-')[1])
    except (ValueError, IndexError):
        logger.warning(f"Invalid season format: {args.season}, using current year")
        season_end_year = 2025  # Default to current year
    
    # Load upcoming matches
    upcoming = load_upcoming_matches(args.input)
    
    if upcoming is None or len(upcoming) == 0:
        logger.error("No upcoming matches found")
        return 1
    
    # Process each fixture
    count = 0
    total_fixtures = len(upcoming)
    
    # Track all matches for final validation
    all_matches_data = []
    
    for idx, fixture in upcoming.iterrows():
        try:
            home_team = fixture['home_team']
            away_team = fixture['away_team']
            fixture_date = fixture.get('date', datetime.now().strftime('%Y-%m-%d'))
            
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
            
            # Process match data and filter out future fixtures
            home_clean = process_match_data(home_df)
            away_clean = process_match_data(away_df)
            
            # Filter out future matches and ensure non-zero stats
            today = datetime.now().date()
            
            if not home_clean.empty and 'date' in home_clean.columns:
                # Convert date to datetime if it's not already
                if not pd.api.types.is_datetime64_dtype(home_clean['date']):
                    home_clean['date'] = pd.to_datetime(home_clean['date'])
                
                # Filter out future matches
                home_clean = home_clean[home_clean['date'].dt.date <= today]
                
                # Ensure we have actual results (non-blank)
                if 'result' in home_clean.columns:
                    home_clean = home_clean[home_clean['result'].notna() & (home_clean['result'] != '')]
                
                # Check for non-zero statistics
                if len(home_clean) > 0 and any(col in home_clean.columns for col in ['gf', 'ga', 'xg']):
                    stats_cols = [col for col in ['gf', 'ga', 'xg'] if col in home_clean.columns]
                    if stats_cols:
                        # Keep only matches with some statistics
                        home_clean = home_clean[home_clean[stats_cols].sum(axis=1) > 0]
            
            if not away_clean.empty and 'date' in away_clean.columns:
                # Convert date to datetime if it's not already
                if not pd.api.types.is_datetime64_dtype(away_clean['date']):
                    away_clean['date'] = pd.to_datetime(away_clean['date'])
                
                # Filter out future matches
                away_clean = away_clean[away_clean['date'].dt.date <= today]
                
                # Ensure we have actual results (non-blank)
                if 'result' in away_clean.columns:
                    away_clean = away_clean[away_clean['result'].notna() & (away_clean['result'] != '')]
                
                # Check for non-zero statistics
                if len(away_clean) > 0 and any(col in away_clean.columns for col in ['gf', 'ga', 'xg']):
                    stats_cols = [col for col in ['gf', 'ga', 'xg'] if col in away_clean.columns]
                    if stats_cols:
                        # Keep only matches with some statistics
                        away_clean = away_clean[away_clean[stats_cols].sum(axis=1) > 0]
            
            # Add team role and track all data
            if not home_clean.empty:
                home_clean['team_role'] = 'home'
                home_clean.to_csv(os.path.join(fixture_dir, f"{home_team}_history.csv"), index=False)
                logger.info(f"Saved {len(home_clean)} completed matches for {home_team}")
                all_matches_data.append(home_clean)
            
            if not away_clean.empty:
                away_clean['team_role'] = 'away'
                away_clean.to_csv(os.path.join(fixture_dir, f"{away_team}_history.csv"), index=False)
                logger.info(f"Saved {len(away_clean)} completed matches for {away_team}")
                all_matches_data.append(away_clean)
            
            # Combine data
            if not home_clean.empty and not away_clean.empty:
                combined = pd.concat([home_clean, away_clean], ignore_index=True)
                # Sort by date (most recent first) and remove duplicates
                if 'date' in combined.columns:
                    combined = combined.sort_values('date', ascending=False)
                combined = combined.drop_duplicates(subset=['match_id', 'team'])
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
                'away_matches_found': len(away_clean),
                'home_latest_match': home_clean['date'].max() if not home_clean.empty and 'date' in home_clean.columns else None,
                'away_latest_match': away_clean['date'].max() if not away_clean.empty and 'date' in away_clean.columns else None
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
    
    # Save all matches to a single file if we have data
    if all_matches_data:
        all_matches = pd.concat(all_matches_data, ignore_index=True)
        # Remove duplicates
        all_matches = all_matches.drop_duplicates(subset=['match_id', 'team'])
        all_matches_file = os.path.join(args.output_dir, "all_matches.csv")
        all_matches.to_csv(all_matches_file, index=False)
        logger.info(f"Saved {len(all_matches)} total matches to {all_matches_file}")
        
        # Validate and print summary
        logger.info("\nMatch Data Summary:")
        if 'date' in all_matches.columns:
            min_date = all_matches['date'].min()
            max_date = all_matches['date'].max()
            logger.info(f"Date range: {min_date} to {max_date}")
        
        if 'result' in all_matches.columns:
            win_count = len(all_matches[all_matches['result'] == 'W'])
            loss_count = len(all_matches[all_matches['result'] == 'L'])
            draw_count = len(all_matches[all_matches['result'] == 'D'])
            logger.info(f"Results: {win_count} wins, {loss_count} losses, {draw_count} draws")
        
        if 'gf' in all_matches.columns and 'ga' in all_matches.columns:
            total_gf = all_matches['gf'].sum()
            total_ga = all_matches['ga'].sum()
            logger.info(f"Goals: {total_gf} for, {total_ga} against")
    
    logger.info(f"Pipeline complete. Processed {count} fixtures")
    return 0