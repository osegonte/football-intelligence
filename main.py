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
    parser = argparse.ArgumentParser(description="Advanced Football Match Scraper")
    
    # Date range options
    parser.add_argument('--start-date', type=str, help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end-date', type=str, help='End date in YYYY-MM-DD format')
    parser.add_argument('--days', type=int, default=7, help='Number of days to fetch (default: 7)')
    
    # Output options
    parser.add_argument('--output-dir', type=str, default='sofascore_data', help='Directory for output files')
    
    # Actions
    parser.add_argument('--stats', action='store_true', help='Print detailed statistics after scraping')
    parser.add_argument('--quiet', action='store_true', help='Reduce output verbosity')
    
    # Source options
    parser.add_argument('--skip-fbref', action='store_true', help='Skip FBref fallback if SofaScore fails')
    
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
    logger.info("Using multiple fallback methods: API → Browser → FBref")
    
    try:
        # Initialize scraper
        scraper = AdvancedSofaScoreScraper()
        
        # Ensure output directory exists
        os.makedirs(args.output_dir, exist_ok=True)
        
        # Fetch matches for date range
        all_matches, total_matches = scraper.fetch_matches_for_date_range(start_date, end_date)
        
        if total_matches > 0:
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