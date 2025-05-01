#!/usr/bin/env python3
"""
Database initialization script for Football Intelligence project.
Creates the PostgreSQL schema and imports data from CSV files.
"""
import os
import sys
import argparse
import logging
from pathlib import Path

# Add the parent directory to the path for imports
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
sys.path.append(parent_dir)

# Import the database connector
from data_processing.db_connector import DatabaseConnector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('db_init.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def find_latest_csv(base_dir="sofascore_data"):
    """
    Find the most recent matches CSV file.
    
    Args:
        base_dir: Base directory to search in
        
    Returns:
        Path to the latest CSV file or None if not found
    """
    # Check if all_matches_latest.csv exists
    latest_path = os.path.join(base_dir, "all_matches_latest.csv")
    if os.path.exists(latest_path):
        return latest_path
    
    # If not, look for date-based filenames
    csv_files = []
    for root, _, files in os.walk(base_dir):
        for file in files:
            if file.startswith("all_matches_") and file.endswith(".csv"):
                csv_files.append(os.path.join(root, file))
    
    if csv_files:
        # Sort by modification time, most recent first
        csv_files.sort(key=os.path.getmtime, reverse=True)
        return csv_files[0]
    
    # Also check the data directory
    data_dir = os.path.join(parent_dir, "data")
    if os.path.exists(data_dir):
        latest_path = os.path.join(data_dir, "all_matches_latest.csv")
        if os.path.exists(latest_path):
            return latest_path
    
    return None

def main():
    parser = argparse.ArgumentParser(description="Initialize the Football Intelligence database")
    
    # Database connection options
    parser.add_argument("--dbname", type=str, default="fbref_stats", help="Database name")
    parser.add_argument("--user", type=str, help="Database user")
    parser.add_argument("--password", type=str, help="Database password")
    parser.add_argument("--host", type=str, default="localhost", help="Database host")
    parser.add_argument("--port", type=int, default=5432, help="Database port")
    
    # Data import options
    parser.add_argument("--csv-file", type=str, help="Path to matches CSV file to import")
    parser.add_argument("--skip-import", action="store_true", help="Skip data import, only create schema")
    
    args = parser.parse_args()
    
    # Initialize database connector
    db = DatabaseConnector(
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        host=args.host,
        port=args.port
    )
    
    # Test connection
    if not db.test_connection():
        logger.error("Failed to connect to the database. Please check your connection settings.")
        return 1
    
    # Connect to the database
    if not db.connect():
        logger.error("Failed to connect to the database. Please check your connection settings.")
        return 1
    
    # Create schema
    if not db.create_schema():
        logger.error("Failed to create database schema.")
        db.disconnect()
        return 1
    
    logger.info("✓ Database schema created successfully.")
    
    # Skip data import if requested
    if args.skip_import:
        logger.info("Skipping data import.")
        db.disconnect()
        return 0
    
    # Find CSV file to import
    csv_file = args.csv_file
    if not csv_file:
        csv_file = find_latest_csv()
    
    if not csv_file or not os.path.exists(csv_file):
        logger.error("No CSV file found to import. Please specify a file with --csv-file.")
        db.disconnect()
        return 1
    
    logger.info(f"Importing data from {csv_file}")
    
    # Import leagues
    league_count = db.import_leagues_from_csv(csv_file)
    logger.info(f"✓ Imported {league_count} leagues.")
    
    # Import teams
    team_count = db.import_teams_from_csv(csv_file)
    logger.info(f"✓ Imported {team_count} teams.")
    
    # Import matches
    match_count = db.import_matches_from_csv(csv_file)
    logger.info(f"✓ Imported {match_count} matches.")
    
    # Close database connection
    db.disconnect()
    
    logger.info("Database initialization complete.")
    return 0

if __name__ == "__main__":
    sys.exit(main())