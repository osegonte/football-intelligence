#!/usr/bin/env python3
"""
Main script for the Football Intelligence platform.
Retrieves reliable match data from FBref with defaults for advanced stats.
"""
import argparse
import os
import sys
import pandas as pd
from datetime import date, datetime, timedelta
import logging
from scrapers.fbref_scraper import FBrefScraper
from data_processing.db_connector import FootballDBConnector, DatabaseConnection

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

def create_data_directories(base_dir="data"):
    """Create necessary directories for storing scraped data"""
    dirs = {
        "base": base_dir,
        "teams": os.path.join(base_dir, "teams"),
        "fixtures": os.path.join(base_dir, "fixtures"),
        "daily": os.path.join(base_dir, "daily")
    }
    
    for dir_path in dirs.values():
        os.makedirs(dir_path, exist_ok=True)
    
    return dirs

def print_match_statistics(matches):
    """Print summary statistics about matches"""
    if not matches:
        logger.info("No matches to summarize")
        return
    
    if isinstance(matches, pd.DataFrame):
        df = matches
    else:
        df = pd.DataFrame(matches)
    
    print("\n=== Match Statistics ===")
    print(f"Total matches: {len(df)}")
    
    if 'competition' in df.columns:
        print("\nCompetitions:")
        for comp, count in df['competition'].value_counts().items():
            print(f"  - {comp}: {count} matches")
    
    if 'team' in df.columns and 'opponent' in df.columns:
        teams = pd.concat([df['team'], df['opponent']]).unique()
        print(f"\nTotal teams: {len(teams)}")
    
    if 'date' in df.columns:
        dates = pd.to_datetime(df['date'])
        print(f"\nDate range: {dates.min().strftime('%Y-%m-%d')} to {dates.max().strftime('%Y-%m-%d')}")
    
    if 'gf' in df.columns:
        print(f"\nTotal goals: {df['gf'].sum()}")

def save_matches_to_csv(matches, filename):
    """Save matches to CSV file"""
    if isinstance(matches, list):
        df = pd.DataFrame(matches)
    else:
        df = matches
    
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    df.to_csv(filename, index=False)
    logger.info(f"Saved {len(df)} matches to {filename}")

def fetch_team_matches(team_name, league="Premier League", num_matches=7, output_dir="data"):
    """Fetch matches for a specific team"""
    logger.info(f"Fetching {num_matches} matches for {team_name} in {league}")
    
    # Initialize database connection
    db_conn = DatabaseConnection()
    
    # Initialize scraper with database connection
    scraper = FBrefScraper(db_conn)
    
    try:
        # Update team in database (this will fetch and store matches)
        match_count = scraper.update_team_in_database(team_name, league, num_matches)
        
        if match_count == 0:
            logger.warning(f"No matches found for {team_name}")
            return None
        
        # Get the matches from database
        db_conn.connect()
        team = db_conn.get_team_by_name(team_name)
        
        if team:
            team_id = team[0]
            # Query recent matches
            query = """
                SELECT date, team_name as team, opponent_name as opponent, venue, 
                       competition, round, result, gf, ga 
                FROM team_match_stats 
                WHERE team_id = %s 
                ORDER BY date DESC 
                LIMIT %s
            """
            matches = db_conn.execute_query(query, (team_id, num_matches))
            
            if matches:
                # Convert to DataFrame
                column_names = ['date', 'team', 'opponent', 'venue', 'competition', 'round', 'result', 'gf', 'ga']
                matches_df = pd.DataFrame(matches, columns=column_names)
                
                # Save to CSV
                csv_file = os.path.join(output_dir, "teams", f"{team_name.replace(' ', '_').lower()}_matches.csv")
                save_matches_to_csv(matches_df, csv_file)
                
                return matches_df
        
        return None
    
    finally:
        db_conn.disconnect()

def fetch_fixture_data(home_team, away_team, league="Premier League", num_matches=7, output_dir="data"):
    """Fetch historical data for both teams in a fixture"""
    logger.info(f"Fetching fixture data for {home_team} vs {away_team}")
    
    # Initialize database connection
    db_conn = DatabaseConnection()
    
    # Initialize scraper with database connection
    scraper = FBrefScraper(db_conn)
    
    # Fetch home team data
    home_df = fetch_team_matches(home_team, league, num_matches, output_dir)
    
    # Fetch away team data
    import time
    time.sleep(5)  # Rate limiting
    away_df = fetch_team_matches(away_team, league, num_matches, output_dir)
    
    if home_df is None and away_df is None:
        logger.warning("No matches found for either team")
        return None
    
    # Create fixture directory
    fixture_name = f"{home_team.replace(' ', '_')}_vs_{away_team.replace(' ', '_')}".lower()
    fixture_dir = os.path.join(output_dir, "fixtures", fixture_name)
    os.makedirs(fixture_dir, exist_ok=True)
    
    # Save team data
    if home_df is not None:
        save_matches_to_csv(home_df, os.path.join(fixture_dir, "home_team_matches.csv"))
    
    if away_df is not None:
        save_matches_to_csv(away_df, os.path.join(fixture_dir, "away_team_matches.csv"))
    
    # Combine data
    combined_df = pd.DataFrame()
    if home_df is not None and away_df is not None:
        combined_df = pd.concat([home_df, away_df], ignore_index=True)
        save_matches_to_csv(combined_df, os.path.join(fixture_dir, "combined_matches.csv"))
    
    return {
        'home_team': home_df,
        'away_team': away_df,
        'combined': combined_df
    }

def main():
    """Main function to fetch and process football matches"""
    parser = argparse.ArgumentParser(description="Football Match Data Collector - Reliable Data Only")
    
    # Command options
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Team command
    team_parser = subparsers.add_parser('team', help='Fetch matches for a specific team')
    team_parser.add_argument('--name', required=True, help='Team name')
    team_parser.add_argument('--league', default='Premier League', help='League name')
    team_parser.add_argument('--matches', type=int, default=7, help='Number of matches to fetch')
    
    # Fixture command
    fixture_parser = subparsers.add_parser('fixture', help='Fetch data for a fixture')
    fixture_parser.add_argument('--home', required=True, help='Home team name')
    fixture_parser.add_argument('--away', required=True, help='Away team name')
    fixture_parser.add_argument('--league', default='Premier League', help='League name')
    fixture_parser.add_argument('--matches', type=int, default=7, help='Number of matches per team')
    
    # Common options
    parser.add_argument('--output-dir', type=str, default='data', help='Directory for output files')
    parser.add_argument('--stats', action='store_true', help='Print match statistics')
    parser.add_argument('--quiet', action='store_true', help='Reduce output verbosity')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Set log level based on verbosity option
    if args.quiet:
        logging.getLogger().setLevel(logging.WARNING)
    
    # Create data directories
    dirs = create_data_directories(args.output_dir)
    
    try:
        if args.command == 'team':
            matches_df = fetch_team_matches(
                args.name,
                args.league,
                args.matches,
                args.output_dir
            )
            
            if args.stats and matches_df is not None and not matches_df.empty:
                print_match_statistics(matches_df)
            
            if matches_df is not None and not matches_df.empty:
                print(f"\nFetched {len(matches_df)} matches for {args.name}")
                print(matches_df[['date', 'opponent', 'venue', 'result', 'gf', 'ga']].to_string(index=False))
        
        elif args.command == 'fixture':
            fixture_data = fetch_fixture_data(
                args.home,
                args.away,
                args.league,
                args.matches,
                args.output_dir
            )
            
            if fixture_data:
                if args.stats:
                    print(f"\n=== {args.home} Statistics ===")
                    if fixture_data['home_team'] is not None:
                        print_match_statistics(fixture_data['home_team'])
                    print(f"\n=== {args.away} Statistics ===")
                    if fixture_data['away_team'] is not None:
                        print_match_statistics(fixture_data['away_team'])
                
                print(f"\nFixture data collected:")
                print(f"- {args.home}: {len(fixture_data['home_team']) if fixture_data['home_team'] is not None else 0} matches")
                print(f"- {args.away}: {len(fixture_data['away_team']) if fixture_data['away_team'] is not None else 0} matches")
        
        logger.info("Data collection completed successfully")
        return 0
        
    except KeyboardInterrupt:
        logger.warning("Operation canceled by user")
        return 1
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

if __name__ == "__main__":
    sys.exit(main())