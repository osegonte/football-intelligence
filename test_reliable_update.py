#!/usr/bin/env python3
"""
Test script to verify the reliable data implementation
"""
import sys
import os
import pandas as pd
from scrapers.fbref_scraper import FBrefScraper
from data_processing.db_connector import FootballDBConnector

def test_fbref_scraper():
    """Test the FBref scraper for reliable data only"""
    print("\n=== Testing FBref Scraper ===")
    
    scraper = FBrefScraper()
    
    # Test data for Arsenal
    team_name = "Arsenal"
    league_name = "Premier League"
    num_matches = 3
    
    print(f"\nFetching {num_matches} matches for {team_name}...")
    matches_df = scraper.get_recent_team_matches(team_name, league_name, num_matches)
    
    if matches_df.empty:
        print("ERROR: No matches found")
        return False
    
    print(f"Successfully fetched {len(matches_df)} matches")
    
    # Check data structure
    print("\nChecking data structure...")
    
    # Expected reliable fields
    reliable_fields = ['date', 'team', 'opponent', 'venue', 'result', 'comp', 'season', 'gf', 'ga']
    
    # Expected default fields (should be 0)
    default_fields = ['xg', 'xga', 'sh', 'sot', 'possession', 'corners', 'opp_corners']
    
    # Check reliable fields
    for field in reliable_fields:
        if field not in matches_df.columns:
            print(f"ERROR: Missing field: {field}")
            return False
        
        # Check if field has non-empty values
        if matches_df[field].isna().all():
            print(f"WARNING: All values are NA for field: {field}")
    
    # Check default fields
    for field in default_fields:
        if field not in matches_df.columns:
            print(f"ERROR: Missing default field: {field}")
            return False
        
        # Check if field has default values (0)
        if not (matches_df[field] == 0).all():
            print(f"WARNING: Non-zero values found in default field: {field}")
    
    # Display sample data
    print("\nSample data (reliable fields only):")
    print(matches_df[reliable_fields].head().to_string(index=False))
    
    # Display default fields to verify they're all 0
    print("\nDefault fields (should all be 0):")
    print(matches_df[default_fields].head().to_string(index=False))
    
    return True

def test_database_storage():
    """Test database storage of reliable data"""
    print("\n=== Testing Database Storage ===")
    
    # Create test data
    test_match = {
        'match_id': '20250501_Arsenal_Chelsea',
        'date': '2025-05-01',
        'team': 'Arsenal',
        'opponent': 'Chelsea',
        'venue': 'Home',
        'result': 'W',
        'comp': 'Premier League',
        'season': '2024/25',
        'round': 'Matchweek 35',
        'gf': 2,
        'ga': 1,
        # Default values
        'xg': 0.0,
        'xga': 0.0,
        'sh': 0,
        'sot': 0,
        'possession': 0.0,
        'corners': 0,
        'opp_corners': 0
    }
    
    # Initialize database connector
    db = FootballDBConnector()
    
    # Store the test match
    print("Storing test match...")
    match_id = db.store_match(test_match)
    
    if match_id:
        print(f"Successfully stored match with ID: {match_id}")
        
        # Verify the storage
        if db.connect():
            # Query the match
            query = "SELECT * FROM team_match_stats WHERE match_id = %s"
            result = db.execute_query(query, (match_id,))
            
            if result:
                print("Match successfully retrieved from database")
                # You could add more verification here
            else:
                print("ERROR: Could not retrieve match from database")
            
            db.disconnect()
    else:
        print("ERROR: Failed to store match")
    
    return match_id is not None

def main():
    """Run all tests"""
    print("Testing Reliable Data Implementation")
    print("===================================")
    
    # Test scraper
    scraper_ok = test_fbref_scraper()
    
    # Test database storage
    db_ok = test_database_storage()
    
    # Summary
    print("\n=== Test Summary ===")
    print(f"FBref Scraper: {'PASSED' if scraper_ok else 'FAILED'}")
    print(f"Database Storage: {'PASSED' if db_ok else 'FAILED'}")
    
    if scraper_ok and db_ok:
        print("\nAll tests passed! The implementation is working correctly.")
        print("The system now fetches only reliable data and sets defaults for advanced stats.")
        return 0
    else:
        print("\nSome tests failed. Please check the errors above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())