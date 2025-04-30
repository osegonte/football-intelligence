#!/usr/bin/env python3
import os
import sys
import subprocess
import shutil

def ensure_data_file_exists():
    """
    Ensure the data file exists by checking various locations
    and copying it to the expected location if needed
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
    
    print("✗ No data file found. Please run the scraper first.")
    return False

def run_dashboard():
    """Run the enhanced football dashboard"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Check if the enhanced dashboard exists
    enhanced_dashboard = os.path.join(script_dir, "dashboard", "enhanced_app.py")
    original_dashboard = os.path.join(script_dir, "dashboard", "app.py")
    
    # Determine which dashboard file to use
    if os.path.exists(enhanced_dashboard):
        dashboard_file = enhanced_dashboard
        print("✓ Using enhanced dashboard")
    elif os.path.exists(original_dashboard):
        dashboard_file = original_dashboard
        print("✓ Using original dashboard")
    else:
        print("✗ Dashboard file not found.")
        return False
    
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
    
    # Check if data file exists
    if not ensure_data_file_exists():
        print("\nWould you like to run the scraper now to collect match data? (y/n)")
        choice = input("> ").strip().lower()
        if choice == 'y':
            script_dir = os.path.dirname(os.path.abspath(__file__))
            main_script = os.path.join(script_dir, "main.py")
            
            if os.path.exists(main_script):
                print("\nRunning scraper to collect match data...")
                try:
                    subprocess.run([sys.executable, main_script, "--days", "7", "--stats"])
                    # Check if the data file now exists
                    if not ensure_data_file_exists():
                        print("✗ Scraper ran but no data file was created.")
                        return
                except Exception as e:
                    print(f"✗ Error running scraper: {e}")
                    return
            else:
                print(f"✗ Main script not found at: {main_script}")
                return
        else:
            print("Dashboard cannot run without data. Exiting.")
            return
    
    # Run the dashboard
    run_dashboard()

if __name__ == "__main__":
    main()