import pandas as pd
import os
from datetime import datetime

def clean_match_data(input_file, output_file=None):
    """
    Clean match data to include only completed matches with statistics
    
    Args:
        input_file: Path to input CSV
        output_file: Path to save cleaned data (if None, will use input_file name with _cleaned suffix)
    """
    if output_file is None:
        base, ext = os.path.splitext(input_file)
        output_file = f"{base}_cleaned{ext}"
    
    print(f"Cleaning {input_file}...")
    
    # Read the CSV
    df = pd.read_csv(input_file)
    print(f"Original rows: {len(df)}")
    
    # Convert date to datetime
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    
    # Filter for past matches only
    today = datetime.now()
    past_matches = df[df['date'] < today] if 'date' in df.columns else df
    print(f"Past matches: {len(past_matches)}")
    
    # Filter for matches with results
    if 'result' in past_matches.columns:
        completed = past_matches[past_matches['result'].notna() & (past_matches['result'] != '')]
        print(f"Completed matches (with result): {len(completed)}")
    else:
        completed = past_matches
    
    # Filter for matches with statistics
    if 'gf' in completed.columns and 'ga' in completed.columns:
        has_stats = completed[(completed['gf'] > 0) | (completed['ga'] > 0)]
        print(f"Matches with statistics: {len(has_stats)}")
    else:
        has_stats = completed
    
    # Save the cleaned data
    has_stats.to_csv(output_file, index=False)
    print(f"Saved {len(has_stats)} cleaned matches to {output_file}")
    
    return has_stats

# Process each CSV file in the match_analysis directory
base_dir = "match_analysis"
for root, dirs, files in os.walk(base_dir):
    for file in files:
        if file.endswith(".csv") and "cleaned" not in file:
            clean_match_data(os.path.join(root, file))