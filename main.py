#!/usr/bin/env python3
"""
Main script for the Football Intelligence platform.
Retrieves football match data from FBref.
"""
import argparse
import os
import sys
import pandas as pd
from datetime import date, datetime, timedelta
import logging
from scrapers.fbref_scraper import FBrefScraper
from scrapers.scraper_utils import save_matches_to_csv, create_data_directories, print_match_statistics

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('football_scraper.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def fetch_historical_matches(team_name, league, num_matches=7, output_dir="data"):
    """
    Fetch historical matches for a specific team
    
    Args:
        team_name: Name of the team
        league: League name
        num_matches: Number of most recent matches to fetch
        output_dir: Directory to save matches
        
    Returns:
        DataFrame with historical matches
    """
    logger.info(f"Fetching {num_matches} historical matches for {team_name} in {league}")
    
    # Initialize scraper
    scraper = FBrefScraper()
    
    # Get historical matches
    historical_df = scraper.get_recent_team_matches(team_name, league, num_matches=num_matches)
    
    if historical_df.empty:
        logger.warning(f"No historical matches found for {team_name}")
        return None
    
    # Save to CSV
    csv_file = os.path.join(output_dir, f"{team_name.replace(' ', '_')}_historical.csv")
    
    # Convert to list of dictionaries
    historical_matches = historical_df.to_dict(orient="records")
    
    # Save matches
    save_matches_to_csv(historical_matches, csv_file)
    
    return historical_df

def fetch_matches_for_date_range(start_date, end_date, leagues=None, output_dir="data"):
    """
    Fetch all matches for a date range
    
    Args:
        start_date: Start date (datetime.date or YYYY-MM-DD string)
        end_date: End date (datetime.date or YYYY-MM-DD string)
        leagues: Optional list of league names to filter by
        output_dir: Directory to save matches
        
    Returns:
        DataFrame with matches for the date range
    """
    # Convert string dates to datetime.date if needed
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    
    logger.info(f"Fetching matches from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    # Initialize scraper
    scraper = FBrefScraper()
    
    # Generate list of dates in the range
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date)
        current_date += timedelta(days=1)
    
    all_matches = []
    all_matches_by_date = {}
    
    # Process each date
    for current_date in date_list:
        date_str = current_date.strftime("%Y-%m-%d")
        
        logger.info(f"Processing date: {date_str}")
        
        # Fetch matches for this date
        matches = scraper.fetch_matches_for_date(date_str, leagues)
        
        if matches:
            all_matches.extend(matches)
            all_matches_by_date[date_str] = matches
            
            # Save daily matches
            daily_file = os.path.join(output_dir, "daily", f"matches_{date_str}.csv")
            save_matches_to_csv(matches, daily_file)
            
            logger.info(f"Found {len(matches)} matches for {date_str}")
        else:
            logger.warning(f"No matches found for {date_str}")
    
    # Save all matches to a combined CSV file
    if all_matches:
        all_matches_file = os.path.join(
            output_dir, 
            f"all_matches_{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}.csv"
        )
        save_matches_to_csv(all_matches, all_matches_file)
        
        # Create a copy as "latest" for easy reference
        latest_file = os.path.join(output_dir, "all_matches_latest.csv")
        import shutil
        if os.path.exists(all_matches_file):
            shutil.copy2(all_matches_file, latest_file)
        
        logger.info(f"Saved {len(all_matches)} total matches to {all_matches_file}")
        
        # Return all matches as DataFrame
        return pd.DataFrame(all_matches), all_matches_by_date
    
    return pd.DataFrame(), {}

def main():
    """Main function to fetch and process football matches"""
    parser = argparse.ArgumentParser(description="Football Match Data Collector")
    
    # Date range options for match collection
    parser.add_argument('--start-date', type=str, help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end-date', type=str, help='End date in YYYY-MM-DD format')
    parser.add_argument('--days', type=int, default=7, help='Number of days to fetch (default: 7)')
    
    # League options
    parser.add_argument('--leagues', type=str, nargs='+', help='Leagues to fetch (e.g. "Premier League" "La Liga")')
    
    # Team-specific options
    parser.add_argument('--team', type=str, help='Team to fetch historical matches for')
    parser.add_argument('--team-league', type=str, help='League of the team')
    parser.add_argument('--history', type=int, default=7, help='Number of historical matches to fetch (default: 7)')
    
    # Output options
    parser.add_argument('--output-dir', type=str, default='data', help='Directory for output files')
    parser.add_argument('--stats', action='store_true', help='Print detailed statistics after scraping')
    parser.add_argument('--quiet', action='store_true', help='Reduce output verbosity')
    
    # Test options
    parser.add_argument('--test', action='store_true', help='Run in test mode (limited requests)')
    
    args = parser.parse_args()
    
    # Set log level based on verbosity option
    if args.quiet:
        logging.getLogger().setLevel(logging.WARNING)
    
    # Create data directories
    data_dirs = create_data_directories(args.output_dir)
    
    try:
        # Determine date range
        today = date.today()
        
        if args.start_date:
            start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        else:
            start_date = today - timedelta(days=1)  # Default to yesterday
        
        if args.end_date:
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        else:
            end_date = start_date + timedelta(days=args.days - 1)
        
        # Check if we're fetching historical team data
        if args.team and args.team_league:
            historical_df = fetch_historical_matches(
                args.team, 
                args.team_league, 
                num_matches=args.history,
                output_dir=args.output_dir
            )
            
            if args.stats and historical_df is not None and not historical_df.empty:
                print_match_statistics(historical_df.to_dict(orient="records"))
        
        # Check if we're fetching date range data
        if not args.team or not args.team_league:
            df, matches_by_date = fetch_matches_for_date_range(
                start_date,
                end_date,
                leagues=args.leagues,
                output_dir=args.output_dir
            )
            
            if args.stats and not df.empty:
                print_match_statistics(df.to_dict(orient="records"))
        
        logger.info("Scraping completed successfully")
        
    except KeyboardInterrupt:
        logger.warning("Operation canceled by user")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())