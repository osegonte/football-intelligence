#!/usr/bin/env python3
"""
Simple script to add corners columns to the match table.
Focuses solely on schema modification without any data processing.
"""
import os
import sys
import argparse
import logging

# Add parent directory to path for imports
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
        logging.FileHandler('schema_update.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def add_corners_columns(db):
    """
    Add corners columns to the match table.
    
    Args:
        db: DatabaseConnector instance
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Connect to database
        if not db.connect():
            logger.error("Failed to connect to database")
            return False
        
        # Check if corners column already exists
        check_query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'match' AND column_name = 'corners'
        """
        
        result = db.execute_query(check_query)
        
        if result and len(result) > 0:
            logger.info("Corners column already exists in match table")
            db.disconnect()
            return True
        
        # Add corners columns
        alter_query = """
        ALTER TABLE match
        ADD COLUMN IF NOT EXISTS corners SMALLINT,
        ADD COLUMN IF NOT EXISTS opp_corners SMALLINT
        """
        
        if db.execute_query(alter_query):
            logger.info("Successfully added corners columns to match table")
            db.disconnect()
            return True
        else:
            logger.error("Failed to add corners columns to match table")
            db.disconnect()
            return False
        
    except Exception as e:
        logger.error(f"Error adding corners columns: {str(e)}")
        if db:
            db.disconnect()
        return False

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Add corners columns to match table")
    
    # Database connection options
    parser.add_argument("--dbname", type=str, default="fbref_stats", help="Database name")
    parser.add_argument("--user", type=str, help="Database user")
    parser.add_argument("--password", type=str, help="Database password")
    parser.add_argument("--host", type=str, default="localhost", help="Database host")
    parser.add_argument("--port", type=int, default=5432, help="Database port")
    
    args = parser.parse_args()
    
    # Initialize database connector
    db = DatabaseConnector(
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        host=args.host,
        port=args.port
    )
    
    # Add corners columns
    if add_corners_columns(db):
        logger.info("Successfully added corners columns")
        return 0
    else:
        logger.error("Failed to add corners columns")
        return 1

if __name__ == "__main__":
    sys.exit(main())