#!/usr/bin/env python3
"""
Database Update Scheduler

This script sets up automated updates for the football statistics database.
It can be run as a standalone service or as a cron job.

Features:
1. Scheduled updates at specified intervals
2. Selective updates based on team popularity or league
3. Failure handling with retries
4. Email notifications (optional)
5. Logging of all activities

Usage:
  python db_update_scheduler.py --mode service  # Run as continuous service
  python db_update_scheduler.py --mode once     # Run once and exit
  
To run as a cron job, add to crontab:
  0 2 * * * /usr/bin/python3 /path/to/db_update_scheduler.py --mode once
"""
import os
import sys
import time
import logging
import argparse
import schedule
import json
import smtplib
import traceback
from email.mime.text import MIMEText
from datetime import datetime, timedelta
import configparser

# Import the database and scraper modules
try:
    # Assuming improved_fbref_scraper.py is in the same directory
    from improved_fbref_scraper import DatabaseConnection, FBrefScraper, load_config
except ImportError:
    # Add parent directory to path if needed
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    sys.path.append(parent_dir)
    from improved_fbref_scraper import DatabaseConnection, FBrefScraper, load_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('db_update_scheduler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class UpdateScheduler:
    """Handles scheduling and execution of database updates"""
    
    def __init__(self, config_file="scheduler_config.ini"):
        """Initialize the scheduler with configuration"""
        self.config = self.load_scheduler_config(config_file)
        
        # Initialize database connection
        self.db = DatabaseConnection(
            dbname=self.config['database']['dbname'],
            user=self.config['database']['user'],
            password=self.config['database']['password'],
            host=self.config['database']['host'],
            port=self.config['database']['port']
        )
        
        # Initialize scraper
        self.scraper = FBrefScraper(
            self.db,
            delay_min=self.config['scraping']['delay_min'],
            delay_max=self.config['scraping']['delay_max']
        )
        
        # Track update status
        self.last_update = None
        self.update_log = []
        self.failures = []
        
        # Create stats directory if it doesn't exist
        if not os.path.exists(self.config['scheduler']['stats_dir']):
            os.makedirs(self.config['scheduler']['stats_dir'])
    
    def load_scheduler_config(self, config_file):
        """Load scheduler-specific configuration"""
        default_config = {
            'database': {
                'dbname': 'fbref_stats',
                'user': 'scraper_user',
                'password': '1759',
                'host': 'localhost',
                'port': 5432
            },
            'scraping': {
                'delay_min': 3,
                'delay_max': 5,
                'matches_per_team': 7
            },
            'scheduler': {
                'update_interval': 24,  # hours
                'update_window_start': '01:00',  # 1 AM
                'update_window_end': '05:00',  # 5 AM
                'max_teams_per_update': 0,  # 0 means no limit
                'stats_dir': 'update_stats',
                'popular_teams_weight': 2,  # Update popular teams 2x more often
                'retry_failed': True,
                'retry_delay': 30,  # minutes
                'max_retries': 3,
                'error_threshold': 5  # Max errors before sending alert
            },
            'notifications': {
                'enabled': False,
                'smtp_server': 'smtp.gmail.com',
                'smtp_port': 587,
                'smtp_user': '',
                'smtp_password': '',
                'recipients': '',
                'send_on_error': True,
                'send_on_success': False
            }
        }
        
        # Load config from file if it exists
        if os.path.exists(config_file):
            try:
                parser = configparser.ConfigParser()
                parser.read(config_file)
                
                # Update default config with values from file
                for section in parser.sections():
                    if section in default_config:
                        for key in parser[section]:
                            if key in default_config[section]:
                                # Convert numeric values
                                if isinstance(default_config[section][key], int):
                                    default_config[section][key] = int(parser[section][key])
                                elif isinstance(default_config[section][key], float):
                                    default_config[section][key] = float(parser[section][key])
                                elif key == 'recipients':
                                    # Convert comma-separated emails to list
                                    default_config[section][key] = [
                                        email.strip() for email in parser[section][key].split(',')
                                    ]
                                elif key in ['retry_failed', 'enabled', 'send_on_error', 'send_on_success']:
                                    # Convert boolean values
                                    default_config[section][key] = parser[section].getboolean(key)
                                else:
                                    default_config[section][key] = parser[section][key]
                
                logger.info(f"Loaded configuration from {config_file}")
            except Exception as e:
                logger.error(f"Error loading config file: {str(e)}")
        else:
            logger.warning(f"Config file not found: {config_file}, using defaults")
            # Create default config file
            try:
                parser = configparser.ConfigParser()
                
                for section, settings in default_config.items():
                    parser[section] = {}
                    for key, value in settings.items():
                        parser[section][key] = str(value)
                
                with open(config_file, 'w') as f:
                    parser.write(f)
                
                logger.info(f"Created default config file: {config_file}")
            except Exception as e:
                logger.error(f"Error creating default config file: {str(e)}")
        
        return default_config
    
    def get_teams_to_update(self):
        """
        Determine which teams need an update based on:
        1. Last update time
        2. Team popularity
        3. League priority
        
        Returns:
            List of team dictionaries to update
        """
        if not self.db.connect():
            logger.error("Failed to connect to database")
            return []
        
        try:
            # Get all teams with last update time
            query = """
            SELECT t.team_id, t.team_name, l.league_name, 
                   (SELECT MAX(scrape_date) FROM match WHERE team_id = t.team_id) as last_update
            FROM team t
            LEFT JOIN league l ON t.league_id = l.league_id
            ORDER BY last_update ASC NULLS FIRST
            """
            
            teams = self.db.execute_query(query)
            
            if not teams:
                logger.warning("No teams found in database")
                return []
            
            # Build list of teams to update
            teams_to_update = []
            now = datetime.now()
            
            # Load popular teams list if it exists
            popular_teams = []
            popular_teams_file = os.path.join(self.config['scheduler']['stats_dir'], 'popular_teams.json')
            
            if os.path.exists(popular_teams_file):
                try:
                    with open(popular_teams_file, 'r') as f:
                        popular_teams = json.load(f)
                except Exception as e:
                    logger.error(f"Error loading popular teams: {str(e)}")
            
            # Determine update threshold based on team popularity
            for team in teams:
                team_id = team[0]
                team_name = team[1]
                league_name = team[2] or "Unknown"
                last_update = team[3]
                
                # Calculate hours since last update
                hours_since_update = None
                if last_update:
                    hours_since_update = (now - last_update).total_seconds() / 3600
                
                # Default update interval
                update_interval = self.config['scheduler']['update_interval']
                
                # Adjust interval for popular teams
                is_popular = any(popular['team_name'].lower() == team_name.lower() for popular in popular_teams)
                if is_popular:
                    update_interval = update_interval / self.config['scheduler']['popular_teams_weight']
                
                # Determine if this team needs an update
                needs_update = (
                    last_update is None or  # Never updated
                    hours_since_update is None or  # No update time recorded
                    hours_since_update >= update_interval  # Update interval exceeded
                )
                
                if needs_update:
                    teams_to_update.append({
                        'team_id': team_id,
                        'team_name': team_name,
                        'league_name': league_name,
                        'last_update': last_update,
                        'hours_since_update': hours_since_update,
                        'is_popular': is_popular
                    })
            
            # Sort by priority (never updated first, then popular teams, then by time since last update)
            teams_to_update.sort(key=lambda t: (
                t['last_update'] is not None,  # Never updated first
                not t['is_popular'],  # Popular teams next
                -1 if t['hours_since_update'] is None else -t['hours_since_update']  # Oldest updates next
            ))
            
            # Apply limit if configured
            max_teams = self.config['scheduler']['max_teams_per_update']
            if max_teams > 0 and len(teams_to_update) > max_teams:
                teams_to_update = teams_to_update[:max_teams]
            
            logger.info(f"Found {len(teams_to_update)} teams to update")
            return teams_to_update
            
        except Exception as e:
            logger.error(f"Error getting teams to update: {str(e)}")
            return []
        finally:
            self.db.disconnect()
    
    def process_failed_updates(self):
        """Retry failed updates if configured"""
        if not self.config['scheduler']['retry_failed'] or not self.failures:
            return
        
        logger.info(f"Processing {len(self.failures)} failed updates")
        
        # Copy failures list and clear the original
        failures = self.failures.copy()
        self.failures = []
        
        # Retry each failed team
        for team_info in failures:
            team_name = team_info['team_name']
            league_name = team_info['league_name']
            retries = team_info.get('retries', 0) + 1
            
            if retries <= self.config['scheduler']['max_retries']:
                logger.info(f"Retrying team {team_name} (retry {retries}/{self.config['scheduler']['max_retries']})")
                
                try:
                    # Update team data
                    match_count = self.scraper.update_team_in_database(
                        team_name,
                        league_name,
                        self.config['scraping']['matches_per_team'],
                        True  # Force update
                    )
                    
                    if match_count > 0:
                        logger.info(f"Successfully updated {team_name} on retry")
                        self.update_log.append({
                            'team_name': team_name,
                            'league_name': league_name,
                            'timestamp': datetime.now().isoformat(),
                            'match_count': match_count,
                            'status': 'success',
                            'retries': retries
                        })
                    else:
                        # Still failed, add back to failures with increased retry count
                        team_info['retries'] = retries
                        self.failures.append(team_info)
                        logger.warning(f"Failed to update {team_name} on retry {retries}")
                except Exception as e:
                    # Update failed, add back to failures with increased retry count
                    team_info['retries'] = retries
                    team_info['error'] = str(e)
                    self.failures.append(team_info)
                    logger.error(f"Error updating {team_name} on retry {retries}: {str(e)}")
                
                # Add delay between retries
                time.sleep(5)
            else:
                logger.error(f"Max retries reached for {team_name}, giving up")
                self.update_log.append({
                    'team_name': team_name,
                    'league_name': league_name,
                    'timestamp': datetime.now().isoformat(),
                    'match_count': 0,
                    'status': 'failed',
                    'retries': retries,
                    'error': team_info.get('error', 'Max retries reached')
                })
        
        # Save update log
        self.save_update_log()
        
        # Check if we need to send error notification
        if len(self.failures) >= self.config['scheduler']['error_threshold']:
            self.send_notification(
                subject=f"Football Database Update: {len(self.failures)} update failures",
                message=f"The following teams could not be updated:\n" + 
                        "\n".join([f"- {team['team_name']} ({team['league_name']}): {team.get('error', 'Unknown error')}" 
                                 for team in self.failures])
            )
    
    def run_update_cycle(self):
        """Run a complete update cycle"""
        logger.info("Starting database update cycle")
        start_time = datetime.now()
        
        # Get list of teams to update
        teams_to_update = self.get_teams_to_update()
        
        if not teams_to_update:
            logger.info("No teams need updating")
            self.last_update = start_time
            return
        
        # Process each team
        success_count = 0
        failure_count = 0
        match_count = 0
        
        for i, team_info in enumerate(teams_to_update):
            team_id = team_info['team_id']
            team_name = team_info['team_name']
            league_name = team_info['league_name']
            
            logger.info(f"Updating team {i+1}/{len(teams_to_update)}: {team_name} ({league_name})")
            
            try:
                # Update team data
                team_match_count = self.scraper.update_team_in_database(
                    team_name,
                    league_name,
                    self.config['scraping']['matches_per_team'],
                    False  # Don't force update
                )
                
                if team_match_count > 0:
                    success_count += 1
                    match_count += team_match_count
                    logger.info(f"Updated {team_match_count} matches for {team_name}")
                    
                    self.update_log.append({
                        'team_name': team_name,
                        'league_name': league_name,
                        'timestamp': datetime.now().isoformat(),
                        'match_count': team_match_count,
                        'status': 'success'
                    })
                else:
                    logger.warning(f"No matches updated for {team_name}")
                    
                    self.update_log.append({
                        'team_name': team_name,
                        'league_name': league_name,
                        'timestamp': datetime.now().isoformat(),
                        'match_count': 0,
                        'status': 'empty'
                    })
            except Exception as e:
                failure_count += 1
                logger.error(f"Error updating {team_name}: {str(e)}")
                
                # Add to failures list for retry
                self.failures.append({
                    'team_id': team_id,
                    'team_name': team_name,
                    'league_name': league_name,
                    'error': str(e),
                    'retries': 0
                })
                
                self.update_log.append({
                    'team_name': team_name,
                    'league_name': league_name,
                    'timestamp': datetime.now().isoformat(),
                    'match_count': 0,
                    'status': 'failed',
                    'error': str(e)
                })
            
            # Only add delay if not the last team
            if i < len(teams_to_update) - 1:
                time.sleep(random.uniform(self.config['scraping']['delay_min'], self.config['scraping']['delay_max']))
        
        # Process any failed updates if configured
        if self.failures and self.config['scheduler']['retry_failed']:
            # Wait before retrying
            retry_delay = self.config['scheduler']['retry_delay'] * 60  # Convert to seconds
            logger.info(f"Waiting {self.config['scheduler']['retry_delay']} minutes before retrying failed updates")
            time.sleep(retry_delay)
            
            self.process_failed_updates()
        
        # Calculate stats
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds() / 60  # minutes
        
        update_stats = {
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'duration_minutes': duration,
            'teams_processed': len(teams_to_update),
            'success_count': success_count,
            'failure_count': failure_count,
            'match_count': match_count,
            'remaining_failures': len(self.failures)
        }
        
        # Save statistics
        stats_file = os.path.join(
            self.config['scheduler']['stats_dir'],
            f"update_stats_{start_time.strftime('%Y%m%d_%H%M%S')}.json"
        )
        
        try:
            with open(stats_file, 'w') as f:
                json.dump(update_stats, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving update stats: {str(e)}")
        
        # Save update log
        self.save_update_log()
        
        # Send notification if configured
        if self.config['notifications']['enabled']:
            if failure_count > 0 and self.config['notifications']['send_on_error']:
                self.send_notification(
                    subject=f"Football Database Update: {failure_count} failures",
                    message=f"Update cycle completed with {success_count} successes and {failure_count} failures.\n" +
                            f"Total matches updated: {match_count}\n" +
                            f"Duration: {duration:.1f} minutes"
                )
            elif self.config['notifications']['send_on_success']:
                self.send_notification(
                    subject=f"Football Database Update: Success",
                    message=f"Update cycle completed successfully.\n" +
                            f"Teams processed: {len(teams_to_update)}\n" +
                            f"Total matches updated: {match_count}\n" +
                            f"Duration: {duration:.1f} minutes"
                )
        
        logger.info(f"Update cycle complete: {success_count} successes, {failure_count} failures, {match_count} matches updated")
        self.last_update = end_time
    
    def save_update_log(self):
        """Save the update log to file"""
        log_file = os.path.join(self.config['scheduler']['stats_dir'], "update_log.json")
        
        try:
            # Load existing log if it exists
            existing_log = []
            if os.path.exists(log_file):
                with open(log_file, 'r') as f:
                    existing_log = json.load(f)
            
            # Append new entries
            combined_log = existing_log + self.update_log
            
            # Keep only the last 1000 entries
            if len(combined_log) > 1000:
                combined_log = combined_log[-1000:]
            
            # Save to file
            with open(log_file, 'w') as f:
                json.dump(combined_log, f, indent=2)
            
            # Clear update log
            self.update_log = []
        except Exception as e:
            logger.error(f"Error saving update log: {str(e)}")
    
    def send_notification(self, subject, message):
        """Send an email notification"""
        if not self.config['notifications']['enabled']:
            return
        
        try:
            recipients = self.config['notifications']['recipients']
            if not recipients:
                logger.warning("No recipients configured for notifications")
                return
            
            smtp_server = self.config['notifications']['smtp_server']
            smtp_port = self.config['notifications']['smtp_port']
            smtp_user = self.config['notifications']['smtp_user']
            smtp_password = self.config['notifications']['smtp_password']
            
            if not smtp_server or not smtp_user or not smtp_password:
                logger.warning("SMTP settings not configured properly")
                return
            
            # Create message
            msg = MIMEText(message)
            msg['Subject'] = subject
            msg['From'] = smtp_user
            msg['To'] = ", ".join(recipients) if isinstance(recipients, list) else recipients
            
            # Send email
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(smtp_user, smtp_password)
                server.send_message(msg)
            
            logger.info(f"Sent notification email: {subject}")
        except Exception as e:
            logger.error(f"Error sending notification: {str(e)}")
    
    def is_within_update_window(self):
        """Check if current time is within the configured update window"""
        now = datetime.now().time()
        
        start_time_str = self.config['scheduler']['update_window_start']
        end_time_str = self.config['scheduler']['update_window_end']
        
        start_time = datetime.strptime(start_time_str, "%H:%M").time()
        end_time = datetime.strptime(end_time_str, "%H:%M").time()
        
        # Handle window that spans midnight
        if start_time < end_time:
            return start_time <= now <= end_time
        else:
            return now >= start_time or now <= end_time
    
    def run_update_check(self):
        """Check if an update is needed and run if appropriate"""
        try:
            # Skip if not within update window
            if not self.is_within_update_window():
                logger.debug("Current time is outside update window")
                return
            
            # Skip if updated within the interval
            if self.last_update:
                hours_since_update = (datetime.now() - self.last_update).total_seconds() / 3600
                if hours_since_update < self.config['scheduler']['update_interval']:
                    logger.debug(f"Last update was {hours_since_update:.1f} hours ago, skipping")
                    return
            
            # Run the update cycle
            self.run_update_cycle()
        except Exception as e:
            logger.error(f"Error in update check: {str(e)}")
            logger.error(traceback.format_exc())
    
    def update_popular_teams(self):
        """
        Update the list of popular teams based on match frequency
        This helps prioritize teams that play more matches
        """
        if not self.db.connect():
            logger.error("Failed to connect to database")
            return
        
        try:
            # Query to get teams with most matches
            query = """
            SELECT t.team_id, t.team_name, l.league_name, COUNT(m.match_id) as match_count
            FROM team t
            LEFT JOIN league l ON t.league_id = l.league_id
            LEFT JOIN match m ON t.team_id = m.team_id
            GROUP BY t.team_id, t.team_name, l.league_name
            ORDER BY match_count DESC
            LIMIT 20
            """
            
            teams = self.db.execute_query(query)
            
            if not teams:
                logger.warning("No teams found for popularity update")
                return
            
            # Convert to list of dictionaries
            popular_teams = []
            for team in teams:
                popular_teams.append({
                    'team_id': team[0],
                    'team_name': team[1],
                    'league_name': team[2] or "Unknown",
                    'match_count': team[3]
                })
            
            # Save to file
            popular_teams_file = os.path.join(self.config['scheduler']['stats_dir'], 'popular_teams.json')
            
            with open(popular_teams_file, 'w') as f:
                json.dump(popular_teams, f, indent=2)
            
            logger.info(f"Updated popular teams list with {len(popular_teams)} teams")
        except Exception as e:
            logger.error(f"Error updating popular teams: {str(e)}")
        finally:
            self.db.disconnect()
    
    def run_service(self):
        """Run as a continuous service with scheduled updates"""
        logger.info("Starting update scheduler service")
        
        # Schedule update check every hour
        schedule.every(1).hours.do(self.run_update_check)
        
        # Schedule popular teams update once a day
        schedule.every(1).days.at("00:15").do(self.update_popular_teams)
        
        # Run once at startup
        try:
            self.update_popular_teams()
            self.run_update_check()
        except Exception as e:
            logger.error(f"Error during initial update: {str(e)}")
        
        # Run the schedule
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
            except KeyboardInterrupt:
                logger.info("Service stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in scheduler loop: {str(e)}")
                time.sleep(300)  # Wait 5 minutes on error
    
    def run_once(self):
        """Run a single update cycle"""
        logger.info("Running one-time update")
        
        try:
            self.update_popular_teams()
            self.run_update_cycle()
            logger.info("Update completed successfully")
        except Exception as e:
            logger.error(f"Error during update: {str(e)}")
            return False
        
        return True

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Database Update Scheduler")
    
    # Mode option
    parser.add_argument("--mode", type=str, choices=["service", "once"], default="once",
                       help="Run mode: service for continuous, once for single update")
    
    # Configuration option
    parser.add_argument("--config", type=str, default="scheduler_config.ini",
                       help="Path to configuration file")
    
    args = parser.parse_args()
    
    # Initialize scheduler
    scheduler = UpdateScheduler(args.config)
    
    # Run in selected mode
    if args.mode == "service":
        scheduler.run_service()
    else:
        success = scheduler.run_once()
        return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())