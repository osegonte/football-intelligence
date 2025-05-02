#!/usr/bin/env python3
"""
Test script to verify football match data in a CSV file.
Checks all important match statistics including xG, shots, and corners.
"""
import os
import sys
import pandas as pd
import argparse
from datetime import datetime

def verify_match_data(csv_file, verbose=True):
    """
    Verify the completeness and consistency of football match data
    
    Args:
        csv_file: Path to the CSV file containing match data
        verbose: Whether to print detailed information
        
    Returns:
        True if verification passed, False otherwise
    """
    # Check if file exists
    if not os.path.exists(csv_file):
        print(f"Error: File '{csv_file}' not found.")
        return False
    
    try:
        # Load the CSV data
        df = pd.read_csv(csv_file)
        
        if verbose:
            print(f"Loaded CSV with {len(df)} rows and {len(df.columns)} columns.")
        
        # Check if we have any data
        if len(df) == 0:
            print("Error: CSV file contains no data.")
            return False
        
        # Check for required columns
        required_columns = ['date', 'team', 'opponent']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"Error: Missing required columns: {missing_columns}")
            return False
        
        # Check for important statistics columns
        stat_columns = [
            'result', 'comp', 'season', 'gf', 'ga', 'xg', 'xga', 
            'sh', 'sot', 'possession', 'corners_for', 'corners_against'
        ]
        missing_stats = [col for col in stat_columns if col not in df.columns]
        
        if missing_stats and verbose:
            print(f"Warning: Missing statistics columns: {missing_stats}")
        
        # Convert date to datetime
        try:
            df['date'] = pd.to_datetime(df['date'])
        except Exception as e:
            print(f"Error converting date column: {str(e)}")
            return False
        
        # Check for matches in the future
        today = datetime.now().date()
        future_matches = df[df['date'].dt.date > today]
        
        if len(future_matches) > 0 and verbose:
            print(f"Warning: Found {len(future_matches)} future matches.")
        
        # Show data summary
        if verbose:
            print("\n=== Data Summary ===")
            
            # Date range
            print(f"Date range: {df['date'].min().date()} to {df['date'].max().date()}")
            
            # Teams
            teams = set(df['team'].unique()) | set(df['opponent'].unique())
            print(f"Number of teams: {len(teams)}")
            
            # Competitions
            if 'comp' in df.columns:
                comps = df['comp'].unique()
                print(f"Competitions: {', '.join(comps)}")
            
            # Completeness of important stats
            present_stats = [col for col in stat_columns if col in df.columns]
            
            if present_stats:
                print("\n=== Statistics Completeness ===")
                for col in present_stats:
                    non_null = df[col].notnull().sum()
                    percentage = (non_null / len(df)) * 100
                    print(f"{col}: {non_null}/{len(df)} values ({percentage:.1f}%)")
            
            # Display matches
            sample_size = min(10, len(df))
            print(f"\n=== Sample of {sample_size} Matches ===")
            
            # Create display columns
            basic_cols = ['date', 'team', 'opponent', 'venue', 'comp', 'season']
            available_basic = [col for col in basic_cols if col in df.columns]
            
            result_cols = ['result', 'gf', 'ga']
            available_result = [col for col in result_cols if col in df.columns]
            
            adv_stats = ['xg', 'xga', 'sh', 'sot', 'possession', 'corners_for', 'corners_against']
            available_stats = [col for col in adv_stats if col in df.columns]
            
            # Keep only enough columns to fit on screen
            display_cols = available_basic + available_result + available_stats
            
            # Display sample data
            sample_df = df.head(sample_size)
            print(sample_df[display_cols].to_string())
            
            # Display match count by team
            if 'team' in df.columns:
                print("\n=== Match Count by Team ===")
                team_counts = df['team'].value_counts()
                for team, count in team_counts.items():
                    print(f"{team}: {count} matches")
            
            # Analysis of advanced stats
            if all(col in df.columns for col in ['xg', 'gf']):
                print("\n=== Expected Goals (xG) Analysis ===")
                total_xg = df['xg'].sum()
                total_gf = df['gf'].sum()
                print(f"Total xG: {total_xg:.1f} vs Actual Goals: {total_gf}")
                print(f"xG Efficiency: {total_gf/total_xg:.2f}")
            
            if all(col in df.columns for col in ['sh', 'sot']):
                print("\n=== Shooting Analysis ===")
                total_sh = df['sh'].sum()
                total_sot = df['sot'].sum()
                print(f"Shots: {total_sh}, Shots on Target: {total_sot}")
                print(f"Shot Accuracy: {(total_sot/total_sh if total_sh > 0 else 0):.2f}")
            
            if all(col in df.columns for col in ['corners_for', 'corners_against']):
                print("\n=== Corner Kick Analysis ===")
                total_cf = df['corners_for'].sum()
                total_ca = df['corners_against'].sum()
                print(f"Corners For: {total_cf}, Corners Against: {total_ca}")
                print(f"Corner Differential: {total_cf - total_ca}")
        
        # If we got here, basic verification passed
        if verbose:
            print("\nVerification completed successfully!")
        return True
        
    except Exception as e:
        print(f"Error verifying match data: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    parser = argparse.ArgumentParser(description='Verify football match data in a CSV file.')
    parser.add_argument('csv_file', help='Path to the CSV file')
    parser.add_argument('--quiet', '-q', action='store_true', help='Reduce output verbosity')
    
    args = parser.parse_args()
    
    # Verify the match data
    success = verify_match_data(args.csv_file, verbose=not args.quiet)
    
    # Return appropriate exit code
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())