#!/usr/bin/env python3
"""
Script to create a comprehensive sample match dataset with all key statistics
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

def create_sample_match_data(output_file="comprehensive_match_data.csv"):
    """
    Create a sample match dataset with all key fields and statistics
    
    Args:
        output_file: Path to save the CSV file
    """
    # Define teams
    teams = ["Arsenal", "Manchester City", "Liverpool", "Manchester United", 
             "Chelsea", "Tottenham Hotspur", "Newcastle United", "Aston Villa"]
    
    # Define competitions
    competitions = ["Premier League", "Champions League", "FA Cup", "League Cup"]
    
    # Generate sample matches
    num_matches = 20
    matches = []
    
    # Start date (recent)
    start_date = datetime(2025, 4, 1)
    
    for i in range(num_matches):
        # Select random home and away teams
        home_idx = np.random.randint(0, len(teams))
        away_idx = np.random.randint(0, len(teams))
        while away_idx == home_idx:  # Ensure different teams
            away_idx = np.random.randint(0, len(teams))
        
        home_team = teams[home_idx]
        away_team = teams[away_idx]
        
        # Randomly select competition
        competition = np.random.choice(competitions)
        
        # Generate match date (sequential)
        match_date = start_date + timedelta(days=i*3)  # Matches every 3 days
        
        # Generate random statistics that make sense
        home_gf = np.random.randint(0, 5)  # 0-4 goals
        away_gf = np.random.randint(0, 4)  # 0-3 goals
        
        # Determine result
        if home_gf > away_gf:
            home_result = "W"
            away_result = "L"
        elif home_gf < away_gf:
            home_result = "L"
            away_result = "W"
        else:
            home_result = "D"
            away_result = "D"
        
        # Generate expected goals (xG) that somewhat correlate with actual goals
        home_xg = max(0, np.random.normal(home_gf, 0.7))
        away_xg = max(0, np.random.normal(away_gf, 0.7))
        
        # Generate shots and shots on target
        home_sh = max(5, int(home_xg * 3) + np.random.randint(0, 10))
        away_sh = max(4, int(away_xg * 3) + np.random.randint(0, 8))
        
        home_sot = min(home_sh, max(home_gf, int(home_sh * 0.4)))
        away_sot = min(away_sh, max(away_gf, int(away_sh * 0.4)))
        
        # Generate possession (adds up to 100%)
        home_poss = min(75, max(25, 50 + np.random.normal(0, 10)))
        away_poss = 100 - home_poss
        
        # Generate corners
        home_corners = np.random.randint(2, 12)
        away_corners = np.random.randint(1, 10)
        
        # Add home team match
        matches.append({
            'date': match_date.strftime('%Y-%m-%d'),
            'team': home_team,
            'opponent': away_team,
            'venue': 'Home',
            'competition': competition,
            'season': '2024/25',
            'round': f"Matchweek {i+1}",
            'result': home_result,
            'gf': home_gf,
            'ga': away_gf,
            'xg': round(home_xg, 2),
            'xga': round(away_xg, 2),
            'sh': home_sh,
            'sot': home_sot,
            'possession': round(home_poss, 1),
            'corners': home_corners,
            'opp_corners': away_corners,
        })
        
        # Add away team match (same match from the other perspective)
        matches.append({
            'date': match_date.strftime('%Y-%m-%d'),
            'team': away_team,
            'opponent': home_team,
            'venue': 'Away',
            'competition': competition,
            'season': '2024/25',
            'round': f"Matchweek {i+1}",
            'result': away_result,
            'gf': away_gf,
            'ga': home_gf,
            'xg': round(away_xg, 2),
            'xga': round(home_xg, 2),
            'sh': away_sh,
            'sot': away_sot,
            'possession': round(away_poss, 1),
            'corners': away_corners,
            'opp_corners': home_corners,
        })
    
    # Create DataFrame and save to CSV
    df = pd.DataFrame(matches)
    
    # Create match_id
    df['match_id'] = df.apply(
        lambda row: f"{row['date']}_{row['team']}_{row['opponent']}",
        axis=1
    )
    
    # Reorder columns
    column_order = [
        'match_id', 'date', 'team', 'opponent', 'venue', 'competition', 'season', 'round',
        'result', 'gf', 'ga', 'xg', 'xga', 'sh', 'sot', 'possession', 'corners', 'opp_corners'
    ]
    df = df[column_order]
    
    # Save to CSV
    df.to_csv(output_file, index=False)
    print(f"Created complete match CSV with all statistics at {os.path.abspath(output_file)}")
    return df

if __name__ == "__main__":
    # You can specify a custom output file as a command-line argument
    import sys
    output_file = "comprehensive_match_data.csv"
    if len(sys.argv) > 1:
        output_file = sys.argv[1]
    
    create_sample_match_data(output_file)