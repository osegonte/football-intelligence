#!/usr/bin/env python3
"""
Simple test script for football match data verification
"""
import pandas as pd
import os
import sys

# Default file path
DEFAULT_FILE = "/Users/osegonte/football-intelligence/comprehensive_match_data.csv"

def test_match_data(file_path):
    """Test match data in the CSV file"""
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"ERROR: File not found: {file_path}")
        return False
    
    print(f"Testing match data in: {file_path}")
    
    # Load the CSV
    try:
        df = pd.read_csv(file_path)
        print(f"Successfully loaded CSV with {len(df)} rows and {len(df.columns)} columns")
    except Exception as e:
        print(f"ERROR loading CSV: {e}")
        return False
    
    # Print column names
    print("\nColumns found in the data:")
    for i, col in enumerate(df.columns, 1):
        print(f"  {i}. {col}")
    
    # Print basic stats
    print("\nBasic statistics:")
    
    # Count teams
    if 'team' in df.columns:
        teams = df['team'].unique()
        print(f"  • Teams: {len(teams)} ({', '.join(teams[:5])}{'...' if len(teams) > 5 else ''})")
    
    # Check for date range
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        print(f"  • Date range: {df['date'].min().strftime('%Y-%m-%d')} to {df['date'].max().strftime('%Y-%m-%d')}")
    
    # Check for competitions
    if 'competition' in df.columns:
        competitions = df['competition'].unique()
        print(f"  • Competitions: {len(competitions)} ({', '.join(competitions)})")
    
    # Check key stats
    print("\nKey statistics availability:")
    key_stats = ['result', 'gf', 'ga', 'xg', 'xga', 'sh', 'sot', 'possession', 'corners', 'opp_corners']
    
    for stat in key_stats:
        if stat in df.columns:
            non_null = df[stat].notna().sum()
            pct = (non_null / len(df)) * 100
            print(f"  • {stat}: {non_null}/{len(df)} values ({pct:.1f}%)")
        else:
            print(f"  • {stat}: Not present")
    
    # Show sample data
    print("\nSample data (first 3 rows):")
    sample_cols = ['date', 'team', 'opponent', 'venue', 'result', 'gf', 'ga', 'xg', 'xga']
    sample_cols = [c for c in sample_cols if c in df.columns]
    
    if sample_cols:
        print(df[sample_cols].head(3))
    else:
        print("None of the expected columns are present")
    
    print("\nTest completed successfully!")
    return True

if __name__ == "__main__":
    # Get file path from command line or use default
    file_path = DEFAULT_FILE
    
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    
    # Run the test
    success = test_match_data(file_path)
    
    if not success:
        sys.exit(1)#!/usr/bin/env python3
"""
Simple test script for football match data verification
"""
import pandas as pd
import os
import sys

# Default file path
DEFAULT_FILE = "/Users/osegonte/football-intelligence/comprehensive_match_data.csv"

def test_match_data(file_path):
    """Test match data in the CSV file"""
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"ERROR: File not found: {file_path}")
        return False
    
    print(f"Testing match data in: {file_path}")
    
    # Load the CSV
    try:
        df = pd.read_csv(file_path)
        print(f"Successfully loaded CSV with {len(df)} rows and {len(df.columns)} columns")
    except Exception as e:
        print(f"ERROR loading CSV: {e}")
        return False
    
    # Print column names
    print("\nColumns found in the data:")
    for i, col in enumerate(df.columns, 1):
        print(f"  {i}. {col}")
    
    # Print basic stats
    print("\nBasic statistics:")
    
    # Count teams
    if 'team' in df.columns:
        teams = df['team'].unique()
        print(f"  • Teams: {len(teams)} ({', '.join(teams[:5])}{'...' if len(teams) > 5 else ''})")
    
    # Check for date range
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        print(f"  • Date range: {df['date'].min().strftime('%Y-%m-%d')} to {df['date'].max().strftime('%Y-%m-%d')}")
    
    # Check for competitions
    if 'competition' in df.columns:
        competitions = df['competition'].unique()
        print(f"  • Competitions: {len(competitions)} ({', '.join(competitions)})")
    
    # Check key stats
    print("\nKey statistics availability:")
    key_stats = ['result', 'gf', 'ga', 'xg', 'xga', 'sh', 'sot', 'possession', 'corners', 'opp_corners']
    
    for stat in key_stats:
        if stat in df.columns:
            non_null = df[stat].notna().sum()
            pct = (non_null / len(df)) * 100
            print(f"  • {stat}: {non_null}/{len(df)} values ({pct:.1f}%)")
        else:
            print(f"  • {stat}: Not present")
    
    # Show sample data
    print("\nSample data (first 3 rows):")
    sample_cols = ['date', 'team', 'opponent', 'venue', 'result', 'gf', 'ga', 'xg', 'xga']
    sample_cols = [c for c in sample_cols if c in df.columns]
    
    if sample_cols:
        print(df[sample_cols].head(3))
    else:
        print("None of the expected columns are present")
    
    print("\nTest completed successfully!")
    return True

if __name__ == "__main__":
    # Get file path from command line or use default
    file_path = DEFAULT_FILE
    
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    
    # Run the test
    success = test_match_data(file_path)
    
    if not success:
        sys.exit(1)