import random
import os
import time
import csv
from datetime import datetime

def get_random_headers():
    """Generate realistic browser headers for requests"""
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0"
    ]
    
    return {
        "User-Agent": random.choice(user_agents),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://www.sofascore.com",
        "Referer": "https://www.sofascore.com/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "If-None-Match": f"W/\"{random.randint(10000, 9999999)}\"",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive"
    }

def create_data_directories(base_dir="fbref_data"):
    """
    Create necessary directories for storing scraped data
    
    Args:
        base_dir: Base directory name for data storage
        
    Returns:
        Dictionary containing paths to all created directories
    """
    dirs = {
        "base": base_dir,
        "teams": os.path.join(base_dir, "teams"),
        "matches": os.path.join(base_dir, "matches"),
        "daily": os.path.join(base_dir, "daily"),
        "raw": os.path.join(base_dir, "raw")
    }
    
    # Create directories if they don't exist
    for dir_path in dirs.values():
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    
    return dirs

def add_random_delay(min_seconds=1, max_seconds=3):
    """
    Add a random delay to appear more human-like
    
    Args:
        min_seconds: Minimum delay in seconds
        max_seconds: Maximum delay in seconds
    """
    delay = min_seconds + random.random() * (max_seconds - min_seconds)
    time.sleep(delay)

def save_matches_to_csv(matches, filename, additional_fields=None):
    """
    Save matches to a CSV file
    
    Args:
        matches: List of match dictionaries
        filename: Output CSV filename
        additional_fields: Optional list of additional field names to include
    """
    if not matches:
        print(f"No matches to save to {filename}")
        return
    
    # Define default fieldnames based on the keys in the first match
    if isinstance(matches, list) and len(matches) > 0:
        fieldnames = list(matches[0].keys())
    else:
        # Default fieldnames if the list is empty
        fieldnames = [
            'match_id', 'date', 'team', 'opponent', 'venue', 'competition',
            'round', 'result', 'gf', 'ga', 'xg', 'xga', 'sh', 'sot',
            'fk', 'pk', 'pkatt', 'possession', 'corners', 'source'
        ]
    
    # Add additional fields if provided
    if additional_fields:
        for field in additional_fields:
            if field not in fieldnames:
                fieldnames.append(field)
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    # Write data to CSV
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for match in matches:
            writer.writerow(match)
    
    print(f"✅ Saved {len(matches)} matches to {filename}")

def format_date_for_filename(start_date, end_date):
    """
    Format date range for use in filenames
    
    Args:
        start_date: Start date (datetime.date)
        end_date: End date (datetime.date)
        
    Returns:
        String in format "YYYYMMDD_to_YYYYMMDD"
    """
    return f"{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}"

def print_match_statistics(all_matches):
    """
    Print statistics about fetched matches
    
    Args:
        all_matches: List of match dictionaries or DataFrame
    """
    if not all_matches:
        print("No matches to analyze")
        return
    
    # Convert to list if it's a DataFrame
    if hasattr(all_matches, 'to_dict'):
        all_matches = all_matches.to_dict(orient='records')
    
    # Group by competition/league
    competitions = {}
    for match in all_matches:
        comp = match.get('competition', match.get('league', 'Unknown'))
        if comp not in competitions:
            competitions[comp] = []
        competitions[comp].append(match)
    
    # Group by team
    teams = {}
    for match in all_matches:
        team = match.get('team', 'Unknown')
        if team not in teams:
            teams[team] = []
        teams[team].append(match)
        
        # Also count opponents
        opponent = match.get('opponent', 'Unknown')
        if opponent not in teams:
            teams[opponent] = []
    
    # Print summary
    print("\n=== Match Statistics ===")
    print(f"Total Matches: {len(all_matches)}")
    
    # Print competitions
    print("\nMatches by Competition:")
    for comp, matches in sorted(competitions.items(), key=lambda x: len(x[1]), reverse=True):
        print(f"  • {comp}: {len(matches)} matches")
    
    # Print top teams
    print("\nTop 10 Teams by Match Count:")
    top_teams = sorted(teams.items(), key=lambda x: len(x[1]), reverse=True)[:10]
    for team, matches in top_teams:
        print(f"  • {team}: {len(matches)} matches")
    
    # Print match statistics
    total_goals = sum(match.get('gf', 0) for match in all_matches)
    total_shots = sum(match.get('sh', 0) for match in all_matches)
    total_shots_on_target = sum(match.get('sot', 0) for match in all_matches)
    
    print("\nMatch Statistics:")
    print(f"  • Total Goals: {total_goals}")
    print(f"  • Total Shots: {total_shots}")
    print(f"  • Total Shots on Target: {total_shots_on_target}")
    if total_shots > 0:
        print(f"  • Shot Conversion Rate: {total_goals / total_shots:.2%}")
    if total_shots_on_target > 0:
        print(f"  • Shots on Target Conversion: {total_goals / total_shots_on_target:.2%}")