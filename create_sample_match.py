import pandas as pd
import os

# Sample match data with all the required columns
matches = [
    {
        'date': '2025-04-29',
        'team': 'Arsenal',
        'opponent': 'Paris S-G',
        'venue': 'Home',
        'result': 'L',
        'comp': 'Champions Lg',
        'season': '2024-25',
        'gf': 0,
        'ga': 1,
        'xg': 1.7,
        'xga': 1.2,
        'sh': 12,
        'sot': 4,
        'possession': 49.0,
        'corners_for': 8,
        'corners_against': 5
    },
    {
        'date': '2025-04-23',
        'team': 'Arsenal',
        'opponent': 'Crystal Palace',
        'venue': 'Home',
        'result': 'D',
        'comp': 'Premier League',
        'season': '2024-25',
        'gf': 2,
        'ga': 2,
        'xg': 1.2,
        'xga': 1.7,
        'sh': 15,
        'sot': 7,
        'possession': 67.0,
        'corners_for': 11,
        'corners_against': 3
    },
    {
        'date': '2025-05-01',
        'team': 'Tottenham Hotspur',
        'opponent': 'Bodø/Glimt',
        'venue': 'Home',
        'result': 'W',
        'comp': 'Europa Lg',
        'season': '2024-25',
        'gf': 3,
        'ga': 1,
        'xg': 3.0,
        'xga': 0.3,
        'sh': 18,
        'sot': 9,
        'possession': 42.0,
        'corners_for': 6,
        'corners_against': 2
    },
    {
        'date': '2025-04-27',
        'team': 'Tottenham Hotspur',
        'opponent': 'Liverpool',
        'venue': 'Away',
        'result': 'L',
        'comp': 'Premier League',
        'season': '2024-25',
        'gf': 1,
        'ga': 5,
        'xg': 0.5,
        'xga': 2.2,
        'sh': 9,
        'sot': 3,
        'possession': 39.0,
        'corners_for': 4,
        'corners_against': 9
    }
]

# Create a DataFrame
df = pd.DataFrame(matches)

# Save to CSV
csv_path = '/Users/osegonte/football-intelligence/match.csv'
df.to_csv(csv_path, index=False)

print(f"Created complete match CSV with all statistics at {csv_path}")