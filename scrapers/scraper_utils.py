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

def create_data_directories(base_dir="sofascore_data"):
    """
    Create necessary directories for storing scraped data
    
    Args:
        base_dir: Base directory name for data storage
        
    Returns:
        Dictionary containing paths to all created directories
    """
    dirs = {
        "base": base_dir,
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
    
    # Define default fieldnames
    fieldnames = [
        'id', 'home_team', 'away_team', 'league', 'country',
        'start_timestamp', 'start_time', 'status', 'venue', 'round', 'source'
    ]
    
    # Add additional fields if provided
    if additional_fields:
        fieldnames.extend(additional_fields)
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    # Write data to CSV
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for match in matches:
            writer.writerow(match)
    
    print(f"✓ Saved {len(matches)} matches to {filename}")

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

def print_match_statistics(all_matches_by_date):
    """
    Print statistics about fetched matches
    
    Args:
        all_matches_by_date: Dictionary mapping date strings to lists of match dictionaries
    """
    if not all_matches_by_date:
        print("No matches to analyze")
        return
    
    # Flatten all matches
    all_matches = []
    for matches in all_matches_by_date.values():
        all_matches.extend(matches)
    
    if not all_matches:
        print("No matches to analyze")
        return
    
    # Group by source
    sources = {}
    for match in all_matches:
        source = match.get('source', 'unknown')
        if source not in sources:
            sources[source] = []
        sources[source].append(match)
    
    # Group by league
    leagues = {}
    for match in all_matches:
        league = match['league']
        if league not in leagues:
            leagues[league] = []
        leagues[league].append(match)
    
    # Group by country
    countries = {}
    for match in all_matches:
        country = match.get('country', 'Unknown')
        if country not in countries:
            countries[country] = []
        countries[country].append(match)
    
    # Print summary
    print("\n=== Match Statistics ===")
    print(f"Total Matches: {len(all_matches)}")
    print(f"Date Range: {min(all_matches_by_date.keys())} to {max(all_matches_by_date.keys())}")
    print(f"Days with Matches: {len(all_matches_by_date)}")
    print(f"Total Leagues: {len(leagues)}")
    print(f"Total Countries/Regions: {len(countries)}")
    
    # Print matches by source
    print("\nMatches by Source:")
    for source, matches in sources.items():
        print(f"  • {source}: {len(matches)} matches")
    
    # Print matches per day
    print("\nMatches per Day:")
    for date_str, matches in sorted(all_matches_by_date.items()):
        print(f"  • {date_str}: {len(matches)} matches")
    
    # Print top leagues
    print("\nTop 10 Leagues by Match Count:")
    top_leagues = sorted(leagues.items(), key=lambda x: len(x[1]), reverse=True)[:10]
    for league, matches in top_leagues:
        print(f"  • {league}: {len(matches)} matches")
    
    # Print top countries
    print("\nTop 10 Countries/Regions by Match Count:")
    top_countries = sorted(countries.items(), key=lambda x: len(x[1]), reverse=True)[:10]
    for country, matches in top_countries:
        print(f"  • {country}: {len(matches)} matches")

def debug_response(response, filename):
    """
    Save API response details for debugging
    
    Args:
        response: Response object from request
        filename: File path to save debugging info
    """
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"Status: {response.status_code}\n")
            f.write(f"Headers: {dict(response.headers)}\n")
            f.write(f"Content: {response.text[:1000]}...\n")
    except Exception as e:
        print(f"Error saving debug info: {str(e)}")

def standardize_match_data(matches, date_str=None):
    """
    Ensure all matches have consistent fields
    
    Args:
        matches: List of match dictionaries
        date_str: Optional date string to add to each match
        
    Returns:
        List of standardized match dictionaries
    """
    for match in matches:
        # Add date if provided
        if date_str and 'date' not in match:
            match['date'] = date_str
            
        # Ensure all matches have common fields, even if empty
        for field in ['id', 'home_team', 'away_team', 'league', 'country', 
                      'start_timestamp', 'start_time', 'status', 'source']:
            if field not in match:
                match[field] = None
    
    return matches