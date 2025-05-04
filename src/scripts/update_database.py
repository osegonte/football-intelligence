#!/usr/bin/env python3
"""
Script to update the PostgreSQL database with new match data.
Can be run periodically as a cron job.
"""
import os
import sys
import argparse
import logging
from datetime import date, timedelta
from pathlib import Path

# Add the parent directory to the path for imports
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
sys.path.append(parent_dir)

# Import required modules
from database.db_connector import DatabaseConnector
from src.scrapers.sofascore import AdvancedSofaScoreScraper
from src.scrapers.utils import save_matches_to_csv, format_date_for_filename

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('db_update.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def fetch_new_matches(start_date, end_date, output_dir="sofascore_data"):
    """
    Fetch new matches using the scraper
    
    Args:
        start_date: Start date to fetch
        end_date: End date to fetch
        output_dir: Directory to save CSV files
        
    Returns:
        Path to the CSV file containing new matches, or None if failed
    """
    try:
        # Initialize scraper
        scraper = AdvancedSofaScoreScraper()
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Fetch matches for date range
        all_matches, total_matches = scraper.fetch_matches_for_date_range(start_date, end_date)
        
        if total_matches > 0:
            # Use the latest CSV file created by the scraper
            latest_file = os.path.join(output_dir, "all_matches_latest.csv")
            
            if os.path.exists(latest_file):
                logger.info(f"Successfully fetched {total_matches} matches")
                return latest_file
            else:
                # Try to find the date-specific file
                date_file = os.path.join(
                    output_dir, 
                    f"all_matches_{format_date_for_filename(start_date, end_date)}.csv"
                )
                
                if os.path.exists(date_file):
                    return date_file
                
                logger.error(f"Failed to locate matches CSV file")
                return None
        else:
            logger.error("Failed to fetch any matches")
            return None
        
    except Exception as e:
        logger.error(f"Error fetching matches: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def update_database(csv_file, dbname="fbref_stats", user=None, password=None, host="localhost", port=5432):
    """
    Update the database with new match data
    
    Args:
        csv_file: Path to the CSV file with match data
        dbname: Database name
        user: Database user
        password: Database password
        host: Database host
        port: Database port
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Initialize database connector
        db = DatabaseConnector(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        
        # Connect to the database
        if not db.connect():
            logger.error("Failed to connect to the database")
            return False
        
        # Import data from CSV
        logger.info(f"Importing leagues from {csv_file}")
        league_count = db.import_leagues_from_csv(csv_file)
        
        logger.info(f"Importing teams from {csv_file}")
        team_count = db.import_teams_from_csv(csv_file)
        
        logger.info(f"Importing matches from {csv_file}")
        match_count = db.import_matches_from_csv(csv_file)
        
        # Close database connection
        db.disconnect()
        
        logger.info(f"Database update complete: {league_count} leagues, {team_count} teams, {match_count} matches")
        return True
        
    except Exception as e:
        logger.error(f"Error updating database: {str(e)}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Update the Football Intelligence database with new match data")
    
    # Date range options
    parser.add_argument('--start-date', type=str, help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end-date', type=str, help='End date in YYYY-MM-DD format')
    parser.add_argument('--days', type=int, default=7, help='Number of days to fetch (default: 7)')
    
    # Database connection options
    parser.add_argument("--dbname", type=str, default="fbref_stats", help="Database name")
    parser.add_argument("--user", type=str, help="Database user")
    parser.add_argument("--password", type=str, help="Database password")
    parser.add_argument("--host", type=str, default="localhost", help="Database host")
    parser.add_argument("--port", type=int, default=5432, help="Database port")
    
    # CSV file option
    parser.add_argument("--csv-file", type=str, help="Use existing CSV file instead of fetching new data")
    
    args = parser.parse_args()
    
    # Determine CSV file to use
    csv_file = args.csv_file
    
    if not csv_file:
        # Determine date range for fetching new data
        today = date.today()
        
        if args.start_date:
            from datetime import datetime
            start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        else:
            start_date = today
        
        if args.end_date:
            from datetime import datetime
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        else:
            end_date = today + timedelta(days=args.days)
        
        logger.info(f"Fetching matches from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Fetch new match data
        csv_file = fetch_new_matches(start_date, end_date)
        
        if not csv_file:
            logger.error("Failed to fetch new match data")
            return 1
    
    # Update the database
    success = update_database(
        csv_file=csv_file,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        host=args.host,
        port=args.port
    )
    
    if success:
        logger.info("Database update completed successfully")
        return 0
    else:
        logger.error("Database update failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())