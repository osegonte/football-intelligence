import os
import subprocess
import sys

def main():
    """
    Run the dashboard application with proper path setup
    """
    # Get the root directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Ensure data directory exists
    os.makedirs(os.path.join(script_dir, "data"), exist_ok=True)
    
    # Check if data file exists
    data_file = os.path.join(script_dir, "data", "all_matches_latest.csv")
    if not os.path.exists(data_file):
        print("Warning: Match data file not found. The dashboard will prompt you to run the scraper first.")
    
    # Run Streamlit
    streamlit_cmd = [sys.executable, "-m", "streamlit", "run", os.path.join(script_dir, "dashboard", "app.py")]
    subprocess.run(streamlit_cmd)

if __name__ == "__main__":
    main()