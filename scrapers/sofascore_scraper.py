import time
import json
import os
import random
from datetime import date, datetime, timedelta
import cloudscraper
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import logging
import sys

# Import utilities from the correct module path
from scrapers.scraper_utils import (
    get_random_headers, 
    create_data_directories, 
    add_random_delay, 
    save_matches_to_csv,
    debug_response,
    standardize_match_data,
    print_match_statistics
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sofascore_scraper.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class AdvancedSofaScoreScraper:
    """
    Advanced scraper for SofaScore that uses multiple methods to bypass anti-bot protections
    """
    
    def __init__(self):
        # Create directories for saving data
        self.directories = create_data_directories()
        self.data_dir = self.directories["base"]
        self.daily_dir = self.directories["daily"]
        self.raw_dir = self.directories["raw"]
        
        # Initialize cloudscraper
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True
            },
            delay=5,  # Delay solving the JavaScript challenge
            captcha={'provider': 'return_response'}  # Return the response if there's a CAPTCHA
        )
        
        # Initialize session cookies
        self.cookies = {}
        
        # Set up proxy list (replace with your actual proxies if needed)
        self.proxies = []
        # Example: self.proxies = [{'http': 'http://user:pass@proxy1.com:8000', 'https': 'https://user:pass@proxy1.com:8000'}]
    
    def get_random_proxy(self):
        """Get a random proxy from the proxy list if available"""
        if not self.proxies:
            return None
        return random.choice(self.proxies)
    
    def initialize_browser_session(self):
        """Initialize a browser session and get cookies for API requests"""
        logger.info("Initializing browser session to get cookies...")
        
        try:
            # Set up Chrome options
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_argument(f"user-agent={random.choice(get_random_headers()['User-Agent'])}")
            
            # Initialize Chrome driver
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Mask WebDriver to avoid detection
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            # Visit SofaScore homepage to get cookies
            driver.get("https://www.sofascore.com/")
            
            # Wait for the page to load
            time.sleep(5)
            
            # Extract cookies
            cookies = driver.get_cookies()
            
            # Close the browser
            driver.quit()
            
            # Add cookies to the session
            self.cookies = {cookie['name']: cookie['value'] for cookie in cookies}
            
            logger.info(f"Successfully obtained {len(cookies)} cookies")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing browser session: {str(e)}")
            return False
    
    def fetch_events_via_api(self, target_date):
        """
        Attempt to fetch events through the API with cookies and cloudscraper
        
        Args:
            target_date: Date string in YYYY-MM-DD format
            
        Returns:
            List of event dictionaries or None if failed
        """
        # API endpoints to try
        endpoints = [
            f"https://api.sofascore.com/api/v1/sport/football/scheduled-events/{target_date}",
            f"https://api.sofascore.com/api/v1/sport/football/events/date/{target_date}",
            f"https://api.sofascore.com/api/v1/football/scheduled-events/date/{target_date}",
            f"https://api.sofascore.com/api/v1/events/date/{target_date}/sport/football",
        ]
        
        # Apply additional headers and cookies to the scraper
        self.scraper.headers.update(get_random_headers())
        
        for cookie_name, cookie_value in self.cookies.items():
            self.scraper.cookies.set(cookie_name, cookie_value)
        
        # Try each endpoint
        for endpoint in endpoints:
            try:
                # Get random proxy if available
                proxy = self.get_random_proxy()
                
                # Add a random delay to appear more human-like
                add_random_delay(2, 5)
                
                logger.info(f"Trying API endpoint: {endpoint}")
                
                # Make the request with cloudscraper
                response = self.scraper.get(
                    endpoint, 
                    proxies=proxy,
                    timeout=20
                )
                
                # Save response details for debugging
                debug_file = os.path.join(self.raw_dir, f"api_response_{target_date}.txt")
                debug_response(response, debug_file)
                
                if response.status_code == 403:
                    logger.warning(f"403 Forbidden for {endpoint}")
                    continue
                
                if response.status_code != 200:
                    logger.warning(f"Status code {response.status_code} for {endpoint}")
                    continue
                
                # Try to parse the JSON response
                try:
                    data = response.json()
                    
                    # Find events data in the response
                    events = None
                    if 'events' in data:
                        events = data['events']
                    elif 'scheduledEvents' in data:
                        events = data['scheduledEvents']
                    elif 'data' in data and isinstance(data['data'], list):
                        events = data['data']
                    
                    if events and len(events) > 0:
                        logger.info(f"Found {len(events)} events using API")
                        return events
                except Exception as json_error:
                    logger.warning(f"Failed to parse JSON from API response: {str(json_error)}")
                    continue
            
            except Exception as e:
                logger.error(f"Error with {endpoint}: {str(e)}")
                continue
        
        logger.error(f"Failed to fetch events via API for {target_date}")
        return None
    
    def fetch_events_via_browser(self, target_date):
        """
        Fetch events directly using a headless browser as a fallback method
        
        Args:
            target_date: Date string in YYYY-MM-DD format
            
        Returns:
            List of event dictionaries or None if failed
        """
        logger.info(f"Attempting to fetch events via browser for {target_date}...")
        
        try:
            # Set up Chrome options
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_argument(f"user-agent={random.choice(get_random_headers()['User-Agent'])}")
            
            # Initialize Chrome driver
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Mask WebDriver to avoid detection
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            # Format the date for URL
            url = f"https://www.sofascore.com/football/{target_date}"
            logger.info(f"Opening URL: {url}")
            
            # Navigate to the football page for the target date
            driver.get(url)
            
            # Wait for the page to load and events to appear
            wait = WebDriverWait(driver, 30)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))
            
            # Give extra time for JavaScript to load all events
            time.sleep(10)
            
            # Save the page source for debugging
            page_source = driver.page_source
            source_file = os.path.join(self.raw_dir, f"page_source_{target_date}.html")
            with open(source_file, 'w', encoding='utf-8') as f:
                f.write(page_source)
            
            # Try to extract event data from the window.__INITIAL_STATE__ variable
            script = """
            try {
                if (window.__INITIAL_STATE__ && window.__INITIAL_STATE__.events) {
                    return JSON.stringify(window.__INITIAL_STATE__.events);
                }
                if (window.__NEXT_DATA__ && window.__NEXT_DATA__.props && window.__NEXT_DATA__.props.pageProps) {
                    return JSON.stringify(window.__NEXT_DATA__.props.pageProps);
                }
                return null;
            } catch (e) {
                return "Error: " + e.toString();
            }
            """
            
            result = driver.execute_script(script)
            
            # Close the driver
            driver.quit()
            
            if result and not result.startswith("Error:"):
                try:
                    # Parse the JSON data
                    json_data = json.loads(result)
                    
                    # Save the raw data
                    raw_file = os.path.join(self.raw_dir, f"browser_data_{target_date}.json")
                    with open(raw_file, 'w', encoding='utf-8') as f:
                        json.dump(json_data, f, indent=2)
                    
                    # Extract events
                    events = []
                    if isinstance(json_data, list):
                        events = json_data
                    elif isinstance(json_data, dict) and 'events' in json_data:
                        events = json_data['events']
                    
                    if events and len(events) > 0:
                        logger.info(f"Found {len(events)} events using browser")
                        return events
                    else:
                        logger.warning("No events found in browser data")
                except Exception as parse_error:
                    logger.error(f"Error parsing browser data: {str(parse_error)}")
            else:
                logger.warning(f"Failed to extract data from browser: {result}")
            
            return None
            
        except Exception as e:
            logger.error(f"Error using browser method: {str(e)}")
            return None
    
    def parse_events(self, events, source="api"):
        """
        Parse events from SofaScore API into a standardized format
        
        Args:
            events: List of event dictionaries from the API
            source: Source of the events (api or browser)
            
        Returns:
            List of standardized match dictionaries
        """
        if not events:
            return []
            
        matches = []
        
        for event in events:
            try:
                # Extract home team
                home_team_name = None
                if 'homeTeam' in event and 'name' in event['homeTeam']:
                    home_team_name = event['homeTeam']['name']
                elif 'home' in event and 'name' in event['home']:
                    home_team_name = event['home']['name']
                
                # Extract away team
                away_team_name = None
                if 'awayTeam' in event and 'name' in event['awayTeam']:
                    away_team_name = event['awayTeam']['name']
                elif 'away' in event and 'name' in event['away']:
                    away_team_name = event['away']['name']
                
                # Extract tournament/league
                tournament_name = 'Unknown League'
                if 'tournament' in event and 'name' in event['tournament']:
                    tournament_name = event['tournament']['name']
                elif 'category' in event and 'name' in event['category']:
                    tournament_name = event['category']['name']
                elif 'league' in event and 'name' in event['league']:
                    tournament_name = event['league']['name']
                
                # Skip if we don't have both team names
                if not home_team_name or not away_team_name:
                    continue
                
                # Extract tournament country/region
                country = 'International'
                if 'tournament' in event and 'category' in event['tournament'] and 'name' in event['tournament']['category']:
                    country = event['tournament']['category']['name']
                elif 'category' in event and 'name' in event['category']:
                    country = event['category']['name']
                
                # Extract start time
                start_time = None
                start_time_formatted = 'Unknown'
                if 'startTimestamp' in event:
                    start_time = event['startTimestamp']
                    try:
                        dt = datetime.fromtimestamp(start_time)
                        start_time_formatted = dt.strftime('%H:%M')
                    except:
                        pass
                
                # Extract event ID
                event_id = event.get('id', 'unknown')
                
                # Extract status
                status = 'Unknown'
                if 'status' in event:
                    if isinstance(event['status'], dict) and 'description' in event['status']:
                        status = event['status']['description']
                    elif isinstance(event['status'], str):
                        status = event['status']
                
                # Create standardized match object
                match = {
                    'id': event_id,
                    'home_team': home_team_name,
                    'away_team': away_team_name,
                    'league': tournament_name,
                    'country': country,
                    'start_timestamp': start_time,
                    'start_time': start_time_formatted,
                    'status': status,
                    'source': source
                }
                
                # Add any additional data if available
                if 'venue' in event:
                    if isinstance(event['venue'], dict) and 'name' in event['venue']:
                        match['venue'] = event['venue']['name']
                    elif isinstance(event['venue'], str):
                        match['venue'] = event['venue']
                
                if 'roundInfo' in event:
                    if isinstance(event['roundInfo'], dict) and 'round' in event['roundInfo']:
                        match['round'] = event['roundInfo']['round']
                
                matches.append(match)
                
            except Exception as e:
                logger.warning(f"Error parsing event: {str(e)}")
                continue
        
        return matches
    
    def fetch_matches_for_date_range(self, start_date, end_date):
        """
        Fetch all football matches for a date range using multiple methods
        
        Args:
            start_date: Start date (datetime.date or YYYY-MM-DD string)
            end_date: End date (datetime.date or YYYY-MM-DD string)
            
        Returns:
            Dictionary mapping date strings to lists of match dictionaries
        """
        # Convert string dates to datetime.date if needed
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        
        # Generate list of dates in the range
        date_list = []
        current_date = start_date
        while current_date <= end_date:
            date_list.append(current_date)
            current_date += timedelta(days=1)
        
        # Initialize browser session to get cookies
        self.initialize_browser_session()
        
        all_matches_by_date = {}
        total_matches = 0
        
        # Import FBref scraper here to avoid circular imports
        # Use absolute imports to avoid circular dependencies
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from scrapers.fbref_scraper import FBrefScraper
        fbref = FBrefScraper()
        
        # Process each date
        for current_date in date_list:
            date_str = current_date.strftime("%Y-%m-%d")
            
            logger.info(f"Processing date: {date_str}")
            
            # Try all methods in sequence until one works
            events = None
            source = None
            
            # Method 1: Try API with cloudscraper
            events = self.fetch_events_via_api(date_str)
            if events:
                source = "api"
            
            # Method 2: If API fails, try browser method
            if not events:
                events = self.fetch_events_via_browser(date_str)
                if events:
                    source = "browser"
            
            # Parse events if we got them from SofaScore
            matches = []
            if events:
                matches = self.parse_events(events, source)
            
            # Method 3: If SofaScore methods fail, try FBref
            if not matches:
                matches = fbref.fetch_matches_for_date(date_str)
            
            if matches:
                # Save matches for this date
                date_file = os.path.join(self.daily_dir, f"matches_{date_str}.csv")
                save_matches_to_csv(matches, date_file)
                
                # Add to overall collection
                all_matches_by_date[date_str] = matches
                total_matches += len(matches)
                
                logger.info(f"Processed {len(matches)} matches for {date_str}")
            else:
                logger.error(f"No matches found for {date_str} with any method")
        
        # Save all matches to a single CSV
        all_matches = []
        for date_str, matches in all_matches_by_date.items():
            for match in matches:
                match['date'] = date_str
                all_matches.append(match)
        
        if all_matches:
            all_file = os.path.join(self.data_dir, f"all_matches_{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}.csv")
            
            # Extended fieldnames for combined file
            fieldnames = [
                'date', 'id', 'home_team', 'away_team', 'league', 'country',
                'start_timestamp', 'start_time', 'status', 'venue', 'round', 'source'
            ]
            
            save_matches_to_csv(all_matches, all_file, additional_fields=['date'])
            
            # Create a copy as "latest" for easy reference
            latest_file = os.path.join(self.data_dir, "all_matches_latest.csv")
            import shutil
            shutil.copy2(all_file, latest_file)
            
            logger.info(f"Saved {len(all_matches)} total matches to {all_file}")
        
        return all_matches_by_date, total_matches