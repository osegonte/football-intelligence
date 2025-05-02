#!/usr/bin/env python3
"""
Enhanced test script for football match data verification
This script checks for completeness of match data including all key statistics
"""
import pandas as pd
import sys
import os
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

def verify_match_data(csv_file):
    """
    Verify the completeness and quality of match data
    
    Args:
        csv_file: Path to CSV file with match data
    """
    # Check if file exists
    if not os.path.exists(csv_file):
        print(f"Error: File {csv_file} does not exist")
        return False
    
    # Load the CSV
    try:
        df = pd.read_csv(csv_file)
        print(f"Loaded CSV with {len(df)} rows and {len(df.columns)} columns.")
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return False
    
    # Convert date to datetime if it's a string
    if 'date' in df.columns and not pd.api.types.is_datetime64_dtype(df['date']):
        df['date'] = pd.to_datetime(df['date'])
    
    # Print data summary
    print("\n=== Data Summary ===")
    if 'date' in df.columns:
        print(f"Date range: {df['date'].min().strftime('%Y-%m-%d')} to {df['date'].max().strftime('%Y-%m-%d')}")
    
    teams = set()
    if 'team' in df.columns:
        teams.update(df['team'].unique())
    if 'opponent' in df.columns:
        teams.update(df['opponent'].unique())
    print(f"Number of teams: {len(teams)}")
    
    # Check for competitions/leagues
    if 'competition' in df.columns:
        print(f"Competitions: {', '.join(df['competition'].unique())}")
    elif 'league' in df.columns:
        print(f"Leagues: {', '.join(df['league'].unique())}")
    
    # Check for seasons
    if 'season' in df.columns:
        print(f"Seasons: {', '.join(str(s) for s in df['season'].unique() if pd.notna(s))}")
    
    # Check completeness of essential statistics
    print("\n=== Statistics Completeness ===")
    essential_stats = [
        'result', 'gf', 'ga', 'xg', 'xga', 'sh', 'sot', 'possession', 
        'corners', 'opp_corners'
    ]
    
    for stat in essential_stats:
        if stat in df.columns:
            non_null_count = df[stat].count()
            percentage = (non_null_count / len(df)) * 100
            print(f"{stat}: {non_null_count}/{len(df)} values ({percentage:.1f}%)")
        else:
            print(f"{stat}: column not present")
    
    # Show a sample of the data
    print("\n=== Sample of Matches ===")
    sample_cols = ['date', 'team', 'opponent', 'venue', 'result', 'gf', 'ga', 'xg', 'xga', 'possession']
    present_cols = [col for col in sample_cols if col in df.columns]
    
    if present_cols:
        print(df[present_cols].head().to_string())
    else:
        print("None of the expected columns are present in the data")
    
    # Count matches by team
    if 'team' in df.columns:
        team_counts = df['team'].value_counts()
        print("\n=== Match Count by Team (Top 5) ===")
        for team, count in team_counts.head(5).items():
            print(f"{team}: {count} matches")
    
    # Generate visualizations if enough data is present
    if len(df) >= 5 and all(stat in df.columns for stat in ['xg', 'gf']):
        print("\n=== Generating Visualizations ===")
        try:
            # Create a directory for visualizations
            viz_dir = "match_visualizations"
            os.makedirs(viz_dir, exist_ok=True)
            
            # Plot xG vs. actual goals
            plt.figure(figsize=(10, 6))
            plt.scatter(df['xg'], df['gf'], alpha=0.7, label='Goals For')
            if 'xga' in df.columns and 'ga' in df.columns:
                plt.scatter(df['xga'], df['ga'], alpha=0.7, label='Goals Against')
            
            # Find the maximum value for the plot limits
            max_values = [df['xg'].max(), df['gf'].max()]
            if 'xga' in df.columns:
                max_values.append(df['xga'].max())
            if 'ga' in df.columns:
                max_values.append(df['ga'].max())
            max_val = max(max(max_values), 1)
            
            plt.plot([0, max_val], [0, max_val], 'k--', label='Perfect Correlation')
            plt.xlabel('Expected Goals (xG)')
            plt.ylabel('Actual Goals')
            plt.title('Expected vs. Actual Goals')
            plt.legend()
            plt.grid(True, alpha=0.3)
            
            # Save the plot
            xg_plot_path = os.path.join(viz_dir, "xg_vs_actual.png")
            plt.savefig(xg_plot_path)
            print(f"Saved xG analysis plot to {os.path.abspath(xg_plot_path)}")
            
            # If possession data is available, create another visualization
            if 'possession' in df.columns and df['possession'].notna().sum() >= 3:
                # Plot possession vs. result
                plt.figure(figsize=(10, 6))
                if 'result' in df.columns and df['result'].notna().sum() >= 3:
                    # Check if we have at least 2 unique results
                    if len(df['result'].unique()) >= 2:
                        sns.boxplot(x='result', y='possession', data=df)
                        plt.title('Possession by Match Result')
                        plt.xlabel('Match Result')
                    else:
                        sns.histplot(df['possession'].dropna(), bins=10, kde=True)
                        plt.title('Possession Distribution')
                else:
                    sns.histplot(df['possession'].dropna(), bins=10, kde=True)
                    plt.title('Possession Distribution')
                plt.ylabel('Possession (%)')
                plt.grid(True, alpha=0.3)
                
                # Save the plot
                poss_plot_path = os.path.join(viz_dir, "possession_analysis.png")
                plt.savefig(poss_plot_path)
                print(f"Saved possession analysis plot to {os.path.abspath(poss_plot_path)}")
        
        except Exception as e:
            print(f"Error generating visualizations: {e}")
            import traceback
            traceback.print_exc()
    
    print("\nVerification completed successfully!")
    return True

if __name__ == "__main__":
    # Check for command line arguments
    if len(sys.argv) < 2:
        # Try some default file paths
        default_paths = [
            "comprehensive_match_data.csv",  # Current directory
            "/Users/osegonte/football-intelligence/comprehensive_match_data.csv",  # User's specified path
            "/Users/osegonte/football-intelligence/match.csv",  # Previously used path
            "match.csv"  # Current directory, alternate name
        ]
        
        # Try each path
        for path in default_paths:
            if os.path.exists(path):
                print(f"Using default file: {path}")
                csv_file = path
                break
        else:
            print("Usage: python test_match_data.py <csv_file>")
            sys.exit(1)
    else:
        csv_file = sys.argv[1]
    
    success = verify_match_data(csv_file)
    
    if not success:
        sys.exit(1)