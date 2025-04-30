#!/usr/bin/env python3
"""
Main script for the Football Intelligence platform.
Retrieves football match data from SofaScore and FBref.
"""
import argparse
import os
from datetime import date, timedelta
import logging
from scrapers.sofascore_scraper import AdvancedSofaScoreScraper
from scrapers.scraper_utils import print_match_statistics

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

def main():
    """Main function to fetch and process football matches"""
    parser = argparse.ArgumentParser(description="Football Match Data Collector")
    
    # Date range options
    parser.add_argument('--start-date', type=str, help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end-date', type=str, help='End date in YYYY-MM-DD format')
    parser.add_argument('--days', type=int, default=7, help='Number of days to fetch (default: 7)')
    
    # Output options
    parser.add_argument('--output-dir', type=str, default='data', help='Directory for output files')
    parser.add_argument('--stats', action='store_true', help='Print detailed statistics after scraping')
    parser.add_argument('--quiet', action='store_true', help='Reduce output verbosity')
    
    args = parser.parse_args()
    
    # Set log level based on verbosity option
    if args.quiet:
        logging.getLogger().setLevel(logging.WARNING)
    
    # Determine date range
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
    
    try:
        # Initialize scraper
        scraper = AdvancedSofaScoreScraper()
        
        # Ensure output directory exists
        os.makedirs(args.output_dir, exist_ok=True)
        
        # Ensure sofascore_data directory exists (used by the scraper)
        os.makedirs('sofascore_data', exist_ok=True)
        
        # Fetch matches for date range
        all_matches, total_matches = scraper.fetch_matches_for_date_range(start_date, end_date)
        
        if total_matches > 0:
            # Copy the latest data file to the specified output directory
            source_file = os.path.join('sofascore_data', 'all_matches_latest.csv')
            target_file = os.path.join(args.output_dir, 'all_matches_latest.csv')
            
            if os.path.exists(source_file):
                import shutil
                shutil.copy2(source_file, target_file)
                logger.info(f"Copied data to {target_file}")
            
            # Print statistics if requested
            if args.stats:
                print_match_statistics(all_matches)
            
            logger.info(f"Successfully fetched {total_matches} matches")
            logger.info(f"Data saved to {scraper.data_dir} directory")
        else:
            logger.error("Failed to fetch any matches")
        
    except KeyboardInterrupt:
        logger.warning("Operation canceled by user")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 1
    
    return 0

if __name__ == "__main__":
    main()