#!/usr/bin/env python3
"""
Minimal test for FBref website connectivity and parsing
"""
import requests
from bs4 import BeautifulSoup
import time

def test_fbref_basic_access():
    """Test basic access to FBref website"""
    print("\n=== FBref Basic Access Test ===\n")
    
    # Add proper headers to avoid being blocked
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://fbref.com/',
        'Connection': 'keep-alive'
    }
    
    # First, test basic access to FBref
    try:
        print("Testing access to FBref homepage...")
        response = requests.get("https://fbref.com/en/", headers=headers)
        print(f"Status code: {response.status_code}")
        if response.status_code == 200:
            print("Successfully accessed FBref homepage")
        else:
            print(f"Failed to access FBref homepage: {response.status_code}")
            return
    except Exception as e:
        print(f"Error accessing FBref homepage: {e}")
        return
    
    # Short delay to avoid rate limiting
    time.sleep(2)
    
    # Next, test access to Premier League stats
    try:
        print("\nTesting access to Premier League stats page...")
        pl_url = "https://fbref.com/en/comps/9/Premier-League-Stats"
        response = requests.get(pl_url, headers=headers)
        print(f"Status code: {response.status_code}")
        
        if response.status_code == 200:
            print("Successfully accessed Premier League stats page")
            
            # Try to parse the page and find teams
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Look for team links
            team_links = {}
            
            # Method 1: Look for standard stats table
            print("\nLooking for team links in standard stats table...")
            table = soup.find("table", id="stats_squads_standard_for")
            
            if table:
                links = table.select("tbody tr td a")
                for link in links:
                    href = link.get("href", "")
                    if "/squads/" in href:
                        team_links[link.text.strip()] = "https://fbref.com" + href
                print(f"Found {len(team_links)} teams in standard stats table")
            else:
                print("Standard stats table not found")
            
            # Method 2: Look in any tables
            if not team_links:
                print("\nLooking for team links in any tables...")
                tables = soup.find_all("table")
                print(f"Found {len(tables)} tables on the page")
                
                for i, t in enumerate(tables):
                    links = t.select("tbody tr td a")
                    team_links_in_table = {}
                    
                    for link in links:
                        href = link.get("href", "")
                        if "/squads/" in href:
                            team_links_in_table[link.text.strip()] = "https://fbref.com" + href
                    
                    if team_links_in_table:
                        print(f"Found {len(team_links_in_table)} team links in table {i}")
                        team_links.update(team_links_in_table)
            
            # Display found teams
            if team_links:
                print(f"\nFound a total of {len(team_links)} teams:")
                for i, (team, url) in enumerate(list(team_links.items())[:5], 1):
                    print(f"{i}. {team}: {url}")
                
                if len(team_links) > 5:
                    print(f"... and {len(team_links) - 5} more")
                
                # Check for Arsenal specifically
                if "Arsenal" in team_links:
                    print("\nFound Arsenal! URL:", team_links["Arsenal"])
                else:
                    print("\nArsenal not found in team links")
                    
                    # Try case-insensitive search
                    arsenal_keys = [k for k in team_links.keys() if "arsenal" in k.lower()]
                    if arsenal_keys:
                        print(f"Found possible matches: {arsenal_keys}")
            else:
                print("No team links found on the page")
                
        else:
            print(f"Failed to access Premier League stats page: {response.status_code}")
    except Exception as e:
        print(f"Error accessing Premier League stats page: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_fbref_basic_access()