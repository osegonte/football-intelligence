"""
This script helps diagnose and fix issues with the Football Intelligence Dashboard.
Save this as debug_dashboard.py in your project's root directory and run it.
"""
import os
import sys
import pandas as pd
import shutil
from datetime import datetime

def check_paths():
    """Check if the required data paths exist"""
    print("=== Checking Data Paths ===")
    
    # Get project root directory (where this script is located)
    root_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"Project root directory: {root_dir}")
    
    # Check potential data file locations
    data_dir = os.path.join(root_dir, "data")
    sofascore_dir = os.path.join(root_dir, "sofascore_data")
    
    # Create data directory if it doesn't exist
    if not os.path.exists(data_dir):
        print(f"Creating missing data directory: {data_dir}")
        os.makedirs(data_dir)
    
    data_file = os.path.join(data_dir, "all_matches_latest.csv")
    sofascore_file = os.path.join(sofascore_dir, "all_matches_latest.csv")
    
    print(f"Checking data file: {data_file} - Exists: {os.path.exists(data_file)}")
    print(f"Checking sofascore file: {sofascore_file} - Exists: {os.path.exists(sofascore_file)}")
    
    # If the data file doesn't exist but the sofascore file does, copy it
    if not os.path.exists(data_file) and os.path.exists(sofascore_file):
        print(f"Copying file from {sofascore_file} to {data_file}")
        shutil.copy2(sofascore_file, data_file)
        print("Copy completed.")
    
    # If both files don't exist, check for any all_matches_*.csv files
    if not os.path.exists(data_file) and not os.path.exists(sofascore_file):
        print("Looking for any all_matches_*.csv files in sofascore_data directory...")
        if os.path.exists(sofascore_dir):
            import glob
            matches_files = glob.glob(os.path.join(sofascore_dir, "all_matches_*.csv"))
            if matches_files:
                # Sort by modification time, most recent first
                matches_files.sort(key=os.path.getmtime, reverse=True)
                latest_file = matches_files[0]
                print(f"Found latest file: {latest_file}")
                print(f"Copying to {data_file}")
                shutil.copy2(latest_file, data_file)
                print("Copy completed.")
            else:
                print("No match data files found in sofascore_data directory.")
        else:
            print(f"sofascore_data directory not found at {sofascore_dir}")
    
    return data_file if os.path.exists(data_file) else None

def check_data_format(data_file):
    """Check if the CSV data format is compatible with the dashboard"""
    print("\n=== Checking Data Format ===")
    
    if not data_file or not os.path.exists(data_file):
        print("No data file to check.")
        return False
    
    try:
        # Read the CSV file
        df = pd.read_csv(data_file)
        print(f"Successfully read CSV file with {len(df)} rows and {len(df.columns)} columns.")
        
        # Check for required columns
        required_columns = ['id', 'home_team', 'away_team', 'league', 'country', 
                           'start_time', 'status', 'date']
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"Missing required columns: {missing_columns}")
            return False
        
        # Check date format
        if 'date' in df.columns:
            try:
                # Try to convert date column to datetime
                df['date'] = pd.to_datetime(df['date'])
                print("Date column successfully converted to datetime format.")
            except Exception as e:
                print(f"Error converting date column: {str(e)}")
                # Attempt to fix date format
                try:
                    # Try different date formats
                    df['date'] = pd.to_datetime(df['date'], errors='coerce')
                    # Fill NaT values with today's date
                    df['date'] = df['date'].fillna(pd.Timestamp.now())
                    # Save fixed data
                    df.to_csv(data_file, index=False)
                    print("Fixed date format issues and saved updated CSV.")
                except Exception as fix_error:
                    print(f"Failed to fix date format: {str(fix_error)}")
                    return False
        
        # Check for other potential issues
        if df.empty:
            print("Warning: DataFrame is empty.")
            return False
        
        print("Data format looks good.")
        return True
        
    except Exception as e:
        print(f"Error checking data format: {str(e)}")
        return False

def fix_common_issues(data_file):
    """Fix common issues with the CSV data"""
    print("\n=== Fixing Common Issues ===")
    
    if not data_file or not os.path.exists(data_file):
        print("No data file to fix.")
        return False
    
    try:
        # Read the CSV file
        df = pd.read_csv(data_file)
        
        # 1. Fix date format if needed
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            # Fill any NaT values with today's date
            today = pd.Timestamp.now()
            df['date'] = df['date'].fillna(today)
            print("Fixed date format.")
        else:
            # If date column is missing, add it
            df['date'] = pd.Timestamp.now()
            print("Added missing date column.")
        
        # 2. Ensure all required columns exist
        required_columns = ['id', 'home_team', 'away_team', 'league', 'country', 
                           'start_time', 'status', 'date']
        
        for col in required_columns:
            if col not in df.columns:
                if col == 'id':
                    df[col] = range(1, len(df) + 1)
                elif col == 'start_time':
                    df[col] = '00:00'
                elif col == 'status':
                    df[col] = 'Not started'
                else:
                    df[col] = 'Unknown'
                print(f"Added missing column: {col}")
        
        # 3. Fix any empty team names
        df['home_team'] = df['home_team'].fillna('Unknown Team')
        df['away_team'] = df['away_team'].fillna('Unknown Team')
        
        # 4. Fix empty league or country values
        df['league'] = df['league'].fillna('Unknown League')
        df['country'] = df['country'].fillna('Unknown')
        
        # Save the fixed data
        df.to_csv(data_file, index=False)
        print(f"Saved fixed data to {data_file}")
        
        return True
        
    except Exception as e:
        print(f"Error fixing data: {str(e)}")
        return False

def run_streamlit_dashboard():
    """Run the Streamlit dashboard"""
    print("\n=== Running Streamlit Dashboard ===")
    
    dashboard_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                                   "dashboard", "app.py")
    
    if not os.path.exists(dashboard_file):
        print(f"Dashboard file not found: {dashboard_file}")
        return False
    
    print(f"Dashboard file found: {dashboard_file}")
    print("Starting Streamlit...")
    
    try:
        # Run the streamlit command
        command = f"{sys.executable} -m streamlit run {dashboard_file}"
        print(f"Executing: {command}")
        
        # Instead of running it directly from this script, print instructions
        print("\nPlease run the dashboard using the following command in your terminal:")
        print(f"\n  {command}\n")
        print("If problems persist, please check the output of this script for issues.")
        
        return True
    except Exception as e:
        print(f"Error preparing to run dashboard: {str(e)}")
        return False

def main():
    """Main function to diagnose and fix dashboard issues"""
    print("=== Football Intelligence Dashboard Fix ===")
    print(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Working directory:", os.getcwd())
    
    # Check if paths exist and are accessible
    data_file = check_paths()
    
    # Check data format
    format_ok = check_data_format(data_file)
    
    # Fix common issues if needed
    if not format_ok and data_file:
        fixed = fix_common_issues(data_file)
        if fixed:
            print("Successfully fixed common data issues.")
        else:
            print("Failed to fix all data issues.")
    
    # Run or provide instructions for running the dashboard
    run_streamlit_dashboard()
    
    print("\n=== Diagnosis Summary ===")
    if data_file:
        print(f"✓ Data file is available at: {data_file}")
    else:
        print("✗ No data file found. Please run the scraper or manually add match data.")
    
    if format_ok:
        print("✓ Data format is compatible with the dashboard.")
    else:
        print("⚠ There may be issues with the data format.")
    
    print("\nThank you for using the Football Intelligence Dashboard Fix tool.")

if __name__ == "__main__":
    main()