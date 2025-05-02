#!/usr/bin/env python3
"""
Test FBref scraper with a random match pairing.
"""
import os
import sys
import random
import subprocess
import pandas as pd
import tempfile
from datetime import datetime

# Premier League teams (current season)
premier_league_teams = [
    "Arsenal", "Aston Villa", "Bournemouth", "Brentford", "Brighton", 
    "Chelsea", "Crystal Palace", "Everton", "Fulham", "Liverpool", 
    "Manchester City", "Manchester United", "Newcastle Utd", 
    "Nottingham Forest", "Southampton", "Tottenham", "West Ham", 
    "Wolverhampton"
]

def create_random_match_csv():
    """Create a CSV file with a random match pairing"""
    # Select two random teams
    selected_teams = random.sample(premier_league_teams, 2)
    home_team = selected_teams[0]
    away_team = selected_teams[1]
    
    print(f"Selected random match: {home_team} vs {away_team}")
    
    # Create a temporary CSV file
    temp_dir = tempfile.gettempdir()
    csv_path = os.path.join(temp_dir, "random_match.csv")
    
    # Write the match data
    with open(csv_path, 'w') as f:
        f.write("home_team,away_team\n")
        f.write(f"{home_team},{away_team}\n")
    
    print(f"Created test file at {csv_path}")
    return csv_path, home_team, away_team

def run_fbref_pipeline(csv_path, output_dir="random_match_test"):
    """Run the FBref pipeline on the random match"""
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Build the command
    cmd = [
        "python", "fbref_pipeline.py",
        "--input", csv_path,
        "--output-dir", output_dir,
        "--matches", "5",  # Limit to 5 matches for testing
        "--delay", "3"     # Shorter delay for testing
    ]
    
    print(f"Running command: {' '.join(cmd)}")
    
    # Run the command
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print("Pipeline completed successfully")
        print(f"Output: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Pipeline failed with code {e.returncode}")
        print(f"Error: {e.stderr}")
        return False

def validate_results(output_dir, home_team, away_team):
    """Validate the scraping results"""
    # Path to expected output files
    fixture_dir = os.path.join(output_dir, f"{home_team}_vs_{away_team}")
    
    if not os.path.exists(fixture_dir):
        print(f"❌ Output directory not found: {fixture_dir}")
        return False
    
    home_csv = os.path.join(fixture_dir, f"{home_team}_history.csv")
    away_csv = os.path.join(fixture_dir, f"{away_team}_history.csv")
    combined_csv = os.path.join(fixture_dir, "combined_history.csv")
    
    # Check if files exist
    files_exist = True
    
    if not os.path.exists(home_csv):
        print(f"❌ Home team file not found: {home_csv}")
        files_exist = False
    
    if not os.path.exists(away_csv):
        print(f"❌ Away team file not found: {away_csv}")
        files_exist = False
    
    if not os.path.exists(combined_csv):
        print(f"❌ Combined history file not found: {combined_csv}")
        files_exist = False
    
    if not files_exist:
        return False
    
    # Required fields to check
    required_fields = [
        'date', 'team', 'opponent', 'venue', 'result', 'competition',
        'gf', 'ga', 'xg', 'xga', 'sh', 'sot', 'possession'
    ]
    
    # Validate each file
    validation_results = []
    
    for file_path, team in [(home_csv, home_team), (away_csv, away_team)]:
        print(f"\nValidating data for {team}:")
        
        try:
            df = pd.read_csv(file_path)
            print(f"- Found {len(df)} matches")
            
            # Check for required fields
            missing_fields = [field for field in required_fields if field not in df.columns]
            if missing_fields:
                print(f"- ❌ Missing fields: {missing_fields}")
                validation_results.append(False)
            else:
                print(f"- ✅ All required fields present")
                
                # Check for data in key fields
                data_percentages = {}
                for field in required_fields:
                    non_null_count = df[field].notna().sum()
                    percentage = (non_null_count / len(df)) * 100 if len(df) > 0 else 0
                    data_percentages[field] = percentage
                    status = "✅" if percentage >= 50 else "⚠️" if percentage > 0 else "❌"
                    print(f"- {status} {field}: {percentage:.1f}% complete")
                
                # Overall assessment
                avg_completeness = sum(data_percentages.values()) / len(data_percentages)
                print(f"- Overall data completeness: {avg_completeness:.1f}%")
                
                validation_results.append(avg_completeness >= 60)  # Pass if at least 60% complete
                
        except Exception as e:
            print(f"Error validating {file_path}: {str(e)}")
            validation_results.append(False)
    
    # Final assessment
    if all(validation_results):
        print("\n✅ Validation PASSED: All required data found")
        return True
    else:
        print("\n❌ Validation FAILED: Some data is missing")
        return False

def main():
    """Main function"""
    # Create random match pairing
    csv_path, home_team, away_team = create_random_match_csv()
    
    # Test timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"match_test_{timestamp}"
    
    # Run the pipeline
    success = run_fbref_pipeline(csv_path, output_dir)
    
    if success:
        # Validate the results
        validate_results(output_dir, home_team, away_team)
    
    print(f"\nTest completed. Output directory: {output_dir}")

if __name__ == "__main__":
    main()