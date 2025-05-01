#!/usr/bin/env python3
"""
Script to run the Football Intelligence dashboard.
Ensures data files exist or database connection is available, then launches the Streamlit app.
"""
import os
import sys
import subprocess
import shutil
import json
import configparser
from pathlib import Path

def load_config():
    """
    Load configuration from config file
    
    Returns:
        Dictionary with configuration values
    """
    config = {
        "data_source": "csv",
        "database": {
            "dbname": "fbref_stats",
            "user": "",
            "password": "",
            "host": "localhost",
            "port": 5432
        }
    }
    
    # Look for configuration file
    config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini")
    
    if os.path.exists(config_file):
        try:
            parser = configparser.ConfigParser()
            parser.read(config_file)
            
            if "data_source" in parser.sections():
                if "type" in parser["data_source"]:
                    config["data_source"] = parser["data_source"]["type"].lower()
            
            if "database" in parser.sections():
                for key in config["database"]:
                    if key in parser["database"]:
                        if key == "port":
                            config["database"][key] = int(parser["database"][key])
                        else:
                            config["database"][key] = parser["database"][key]
        except Exception as e:
            print(f"Warning: Failed to load config file: {e}")
    
    return config

def ensure_data_file_exists():
    """
    Ensure the data file exists by checking various locations
    and copying it to the expected location if needed
    
    Returns:
        True if a data file exists, False otherwise
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Expected data locations
    data_dir = os.path.join(script_dir, "data")
    os.makedirs(data_dir, exist_ok=True)
    
    expected_data_file = os.path.join(data_dir, "all_matches_latest.csv")
    
    # Check if the file already exists in the expected location
    if os.path.exists(expected_data_file):
        print(f"✓ Data file found at: {expected_data_file}")
        return True
    
    # Try to find the file in the sofascore_data directory
    sofascore_dir = os.path.join(script_dir, "sofascore_data")
    
    if os.path.exists(sofascore_dir):
        # Look for the latest data file
        latest_file = None
        
        # First check if there's an "all_matches_latest.csv" file
        sofascore_latest = os.path.join(sofascore_dir, "all_matches_latest.csv")
        if os.path.exists(sofascore_latest):
            latest_file = sofascore_latest
        else:
            # Otherwise, look for the most recent file matching the pattern "all_matches_*.csv"
            import glob
            data_files = glob.glob(os.path.join(sofascore_dir, "all_matches_*.csv"))
            if data_files:
                # Sort by modification time, most recent first
                data_files.sort(key=os.path.getmtime, reverse=True)
                latest_file = data_files[0]
        
        if latest_file:
            # Copy the file to the expected location
            shutil.copy2(latest_file, expected_data_file)
            print(f"✓ Copied data file from: {latest_file}")
            print(f"  to: {expected_data_file}")
            return True
    
    print("✗ No data file found.")
    return False

def test_database_connection(db_config):
    """
    Test PostgreSQL database connection
    
    Args:
        db_config: Dictionary with database connection parameters
        
    Returns:
        True if connection successful, False otherwise
    """
    try:
        # Import the DatabaseConnector class
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        from data_processing.db_connector import DatabaseConnector
        
        # Initialize database connector
        db = DatabaseConnector(
            dbname=db_config["dbname"],
            user=db_config["user"] or None,
            password=db_config["password"] or None,
            host=db_config["host"],
            port=db_config["port"]
        )
        
        # Test connection
        return db.test_connection()
    except Exception as e:
        print(f"✗ Database connection error: {e}")
        return False

def run_scraper():
    """
    Run the scraper to fetch new data
    
    Returns:
        True if successful, False otherwise
    """
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        main_script = os.path.join(script_dir, "main.py")
        
        if os.path.exists(main_script):
            print("\nRunning scraper to collect match data...")
            subprocess.run([sys.executable, main_script, "--days", "7", "--stats"])
            
            # Check if the data file now exists
            if ensure_data_file_exists():
                print("✓ Scraper ran and created data file successfully.")
                return True
            else:
                print("✗ Scraper ran but no data file was created.")
                return False
        else:
            print(f"✗ Main script not found at: {main_script}")
            return False
    except Exception as e:
        print(f"✗ Error running scraper: {e}")
        return False

def setup_env_vars(db_config):
    """
    Set environment variables for database connection
    
    Args:
        db_config: Dictionary with database connection parameters
    """
    if db_config["user"]:
        os.environ["DB_USER"] = db_config["user"]
    if db_config["password"]:
        os.environ["DB_PASSWORD"] = db_config["password"]

def run_dashboard(use_db=False, db_config=None):
    """
    Run the football dashboard
    
    Args:
        use_db: Whether to use database mode
        db_config: Database configuration dict
        
    Returns:
        True if successful, False otherwise
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Check for the dashboard file
    dashboard_file = os.path.join(script_dir, "dashboard", "app.py")
    
    if not os.path.exists(dashboard_file):
        print(f"✗ Dashboard file not found at: {dashboard_file}")
        return False
    
    # Set up environment variables if using database
    if use_db and db_config:
        setup_env_vars(db_config)
        os.environ["USE_DATABASE"] = "true"
    else:
        os.environ["USE_DATABASE"] = "false"
    
    # Run the dashboard using streamlit
    try:
        cmd = [sys.executable, "-m", "streamlit", "run", dashboard_file]
        subprocess.run(cmd)
        return True
    except Exception as e:
        print(f"✗ Error running dashboard: {e}")
        return False

def main():
    print("🚀 Starting Football Intelligence Dashboard")
    print("=============================================")
    
    # Load configuration
    config = load_config()
    use_db = config["data_source"].lower() == "database"
    
    data_available = False
    
    # Check data source availability
    if use_db:
        print("Using database as data source...")
        
        # Test database connection
        if test_database_connection(config["database"]):
            print("✓ Database connection successful.")
            data_available = True
        else:
            print("✗ Database connection failed.")
            
            print("\nWould you like to:")
            print("1. Try using CSV files instead")
            print("2. Set up the database now")
            print("3. Exit")
            
            choice = input("> ").strip()
            
            if choice == "1":
                use_db = False
                # Check if CSV file exists
                if ensure_data_file_exists():
                    data_available = True
                else:
                    # Try to run the scraper to get CSV data
                    print("\nNo CSV data file found. Would you like to run the scraper now? (y/n)")
                    choice = input("> ").strip().lower()
                    if choice == 'y':
                        data_available = run_scraper()
                    else:
                        data_available = False
            elif choice == "2":
                # Run database initialization script
                print("\nRunning database initialization script...")
                
                # Get database credentials
                dbname = input("Database name [fbref_stats]: ").strip() or "fbref_stats"
                user = input("Database user: ").strip()
                password = input("Database password: ").strip()
                host = input("Database host [localhost]: ").strip() or "localhost"
                port = input("Database port [5432]: ").strip() or "5432"
                
                try:
                    port = int(port)
                except:
                    port = 5432
                
                # Update config
                config["database"]["dbname"] = dbname
                config["database"]["user"] = user
                config["database"]["password"] = password
                config["database"]["host"] = host
                config["database"]["port"] = port
                
                # Run init_database.py script
                script_dir = os.path.dirname(os.path.abspath(__file__))
                init_script = os.path.join(script_dir, "scripts", "init_database.py")
                
                if os.path.exists(init_script):
                    try:
                        cmd = [
                            sys.executable, 
                            init_script,
                            "--dbname", dbname,
                            "--host", host,
                            "--port", str(port)
                        ]
                        
                        if user:
                            cmd.extend(["--user", user])
                        
                        if password:
                            cmd.extend(["--password", password])
                        
                        result = subprocess.run(cmd)
                        
                        if result.returncode == 0:
                            print("✓ Database initialized successfully.")
                            data_available = True
                        else:
                            print("✗ Database initialization failed.")
                            data_available = False
                    except Exception as e:
                        print(f"✗ Error running database initialization: {e}")
                        data_available = False
                else:
                    print(f"✗ Database initialization script not found at: {init_script}")
                    data_available = False
            else:
                print("Exiting.")
                return
    else:
        print("Using CSV files as data source...")
        
        # Check if CSV file exists
        if ensure_data_file_exists():
            data_available = True
        else:
            # Try to run the scraper to get CSV data
            print("\nNo data file found. Would you like to run the scraper now? (y/n)")
            choice = input("> ").strip().lower()
            if choice == 'y':
                data_available = run_scraper()
            else:
                data_available = False
    
    # Check if data is available
    if not data_available:
        print("\nDashboard cannot run without data. Exiting.")
        return
    
    # Run the dashboard
    run_dashboard(use_db=use_db, db_config=config["database"])

if __name__ == "__main__":
    main()